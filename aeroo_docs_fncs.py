#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
################################################################################
#
# Copyright (c) 2009-2014 Alistek ( http://www.alistek.com ) All Rights Reserved.
#                    General contacts <info@alistek.com>
#
# WARNING: This program as such is intended to be used by professional
# programmers who take the whole responsability of assessing all potential
# consequences resulting from its eventual inadequacies and bugs
# End users who are looking for a ready-to-use solution with commercial
# garantees and support are strongly adviced to contract a Free Software
# Service Company
#
# This program is Free Software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
#
################################################################################
import io
import logging
import base64
from hashlib import md5
from random import randint
from os import path, rename, getpid
from time import time, sleep
from jsonrpc2 import JsonRpcException
from DocumentConverter import DocumentConverter, DocumentConversionException
from PyPDF2 import PdfFileMerger, utils
import tempfile

MAXINT = 9223372036854775807

filters = {'pdf': 'writer_pdf_Export',   # PDF - Portable Document Format
           'odt': 'writer8',  # ODF Text Document
           'ods': 'calc8',   # ODF Spreadsheet
           'doc': 'MS Word 97',  # Microsoft Word 97/2000/XP
           'xls': 'MS Excel 97',  # Microsoft Excel 97/2000/XP
           'csv': 'Text - txt - csv (StarCalc)',  # Text CSV
           }


class AccessException(Exception):
    pass


class NoidentException(Exception):
    pass


class NodataException(Exception):
    pass


class NoOfficeConnection(Exception):
    pass

logger = logging.getLogger('main')


class OfficeService(object):

    def __init__(self, oo_host, oo_port, spool_dir, auth_type):
        self.oo_host = oo_host
        self.oo_port = oo_port
        self.spool_path = spool_dir + '/%s'
        self.auth = auth_type
        self._init_conn()

    def _init_conn(self):
        try:
            self.oservice = DocumentConverter(self.oo_host, self.oo_port)
        except DocumentConversionException as e:
            self.oservice = None
            logger.warning("Failed to initiate OpenOffice/LibreOffice connection.")

    def _conn_healthy(self):
        if hasattr(self, 'oservice'):
            if self.oservice is not None:
                return True
        else:
            self.oservice = None
        attempt = 0
        while self.oservice is None and attempt < 3:
            attempt += 1
            self._init_conn()
            if self.oservice is not None:
                return True
            sleep(3)
        message = 'Failed to initiate connection to OpenOffice/LibreOffice three times in a row.'
        logger.warning(message)
        raise NoOfficeConnection(message)

    def _chktime(self, start_time):
        return '%s s' % str(round(time() - start_time, 6))

    def get_file(self, ident, username=None, password=None):
        if not self.auth(username, password):
            raise AccessException('Access denied.')
        file_data = self._readFile(ident)
        return base64.b64encode(file_data).decode('utf8')

    def convert(self, data=False, identifier=False, in_mime=False, out_mime=False, username=None, password=None):
        if not self.auth(username, password):
            raise AccessException('Access denied.')
        start_time = time()
        logger.debug('Openning identifier: %s' % identifier)
        if data is not False:
            data = base64.b64decode(data)
        elif identifier is not False:
            data = self._readFile(identifier)
        else:
            raise NoidentException('Wrong or no identifier.')
        logger.debug("  read file %s" % self._chktime(start_time))
        self._conn_healthy()
        logger.debug("  connection test ok %s" % self._chktime(start_time))
        infilter = filters.get(in_mime, False)
        outfilter = filters.get(out_mime, False)
        self.oservice.putDocument(data, filter_name=infilter, read_only=False)
        logger.debug("  upload document to office %s" % self._chktime(start_time))
        try:
            conv_data = self.oservice.saveByStream(filter_name=outfilter)
            logger.debug("  download converted document %s" % self._chktime(start_time))
        except Exception as e:
            logger.debug("  conversion failed %s Exception: %s" % (self._chktime(start_time), str(e)))
            self.oservice.closeDocument()
            logger.debug("  emergency close document %s" % self._chktime(start_time))
            raise e
        else:
            self.oservice.closeDocument()
            logger.debug("  close document %s" % self._chktime(start_time))
        return base64.b64encode(conv_data).decode('utf8')

    def _md5(self, data):
        return md5(data.encode()).hexdigest()

    def upload(self, data=False, is_last=False, identifier=False, username=None, password=None):
        logger = logging.getLogger('main')
        logger.debug('Upload identifier: %s' % identifier)
        try:
            start_time = time()

            if not self.auth(username, password):
                raise AccessException('Access denied.')
            # NOTE:md5 conversion on file operations to prevent path injection attack
            if identifier and not path.isfile(self.spool_path % '_' + self._md5(str(identifier))):
                raise NoidentException('Wrong or no identifier.')
            elif data is False:
                raise NodataException('No data to be converted.')

            fname, identifier = self._get_filename_and_identifier(identifier)
            with open(self.spool_path % '_' + fname, "a") as tmpfile:
                tmpfile.write(data)
            logger.debug("  chunk finished %s" % self._chktime(start_time))
            if is_last:
                rename(self.spool_path % '_' + fname, self.spool_path % fname)
                logger.debug("  file finished")
            return {'identifier': identifier}
        except AccessException as e:
            raise e
        except NoidentException as e:
            raise e
        except NodataException as e:
            raise e
        except:
            import sys
            import traceback
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            traceback.print_exception(exceptionType, exceptionValue,
                                      exceptionTraceback, limit=2, file=sys.stdout)

    def _get_filename_and_identifier(self, force_identifier=None):
        fname = ''
        # generate random identifier
        identifier = force_identifier
        while not identifier:
            new_ident = randint(1, MAXINT)
            fname = self._md5(str(new_ident))
            logger.debug('  assigning new identifier %s' % new_ident)
            # check if there is any other such files
            identifier = not path.isfile(self.spool_path % '_' + fname) \
                         and not path.isfile(self.spool_path % fname) \
                         and new_ident or False
        fname = fname or self._md5(str(identifier))
        return fname, identifier

    def _readFile(self, ident):
        spool_file_name = self._md5(str(ident))
        logger.debug("> read id %s for spool name %s", ident, spool_file_name)
        with open(self.spool_path % spool_file_name, "r") as tmpfile:
            data = tmpfile.read()
        return base64.b64decode(data)

    def _readFileUTF8(self, ident):
        spool_file_name = self._md5(str(ident))
        logger.debug("> read in utf-8 id %s for spool name %s", ident, spool_file_name)
        return open(self.spool_path % spool_file_name, 'rb').read().decode('utf8')

    def _readFiles(self, idents):
        logger = logging.getLogger('main')
        for ident in idents:
            start_time = time()
            data = self._readFile(ident)
            logger.debug("    read next file: %s +%s" % (ident, self._chktime(start_time)))
            yield data

    def join(self, idents, in_mime=False, out_mime=False, username=None, password=None):
        logger.debug('Join %s identifiers: %s' % (str(len(idents)), str(idents)))
        if not self.auth(username, password):
            raise AccessException('Access denied.')

        if in_mime == out_mime == 'pdf':
            return self._join_pdf_to_pdf(idents, in_mime, out_mime)

        return self._join_default(idents, in_mime, out_mime)

    def _join_default(self, idents, in_mime=False, out_mime=False):
        """
        Join odt document to pdf or to another odt document
        :param idents: the aeroo_resport se4rvice file identifier return by upload function to join
        :param in_mime: accepted odt maybe other
        :param out_mime: accpeted odt, pdf
        :return: the join file result or raise if an error has occured
        """
        start_time = time()
        ident = idents.pop(0)
        data = self._readFile(ident)
        logger.debug("  read first file %s" % self._chktime(start_time))
        self._conn_healthy()
        logger.debug("  connection test ok %s" % self._chktime(start_time))
        try:
            infilter = filters.get(in_mime, False) or 'writer8'
            outfilter = filters.get(out_mime, False)
            self.oservice.putDocument(data, filter_name=infilter, read_only=True)
            logger.debug("  upload first document to office %s" % self._chktime(start_time))
            self.oservice.appendDocuments(self._readFiles(idents), filter_name=infilter)
            result_data = self.oservice.saveByStream(outfilter)
        except Exception as e:
            logger.debug("  conversion failed %s Exception: %s" % (self._chktime(start_time), str(e)))
            self.oservice.closeDocument()
            logger.debug("  emergency close document %s" % self._chktime(start_time))
            raise e
        else:
            self.oservice.closeDocument()
            logger.debug("  close document %s" % self._chktime(start_time))
        logger.debug("  join finished %s" % self._chktime(start_time))
        return base64.b64encode(result_data).decode('utf8')

    def _join_pdf_to_pdf(self, idents, in_mime, out_mime):
        logger.debug('Merge %s pdf identifiers: %s' % (str(len(idents)), str(idents)))
        try:
            new_idents = []
            while idents:
                to_process = idents[:100]
                idents = idents[100:]
                start_time = time()
                out_file_name, new_ident = self._merge_idents(to_process)
                new_idents.append(new_ident)
                logger.debug(">write merged file %s in %s", out_file_name, self._chktime(start_time))

            if len(new_idents) > 1:
                return self._join_pdf_to_pdf(new_idents, in_mime=in_mime, out_mime=out_mime)
            return self._readFileUTF8(new_idents[0])
        except Exception as e:
            logger.info(e)
            logger.exception(e)
            raise e


    def _merge_idents(self, idents):
        merger = PdfFileMerger()
        out_io = io.BytesIO()
        for ident in idents:
            file_data = self._readFile(ident)
            merger.append(io.BytesIO(file_data))
        merger.write(out_io)
        merger.close()
        out_file_name, new_ident = self._get_filename_and_identifier()
        with open(self.spool_path % out_file_name, "wb") as outFile:
            outFile.write(base64.encodebytes(out_io.getvalue()))
        out_io.close()
        return out_file_name, new_ident

