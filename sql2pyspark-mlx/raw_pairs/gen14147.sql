SELECT id, paid_at, status, ROW_NUMBER() OVER (PARTITION BY MONTH(paid_at), status ORDER BY paid_at, id) paid_at_row FROM payments WHERE status = 'SUCCESS' ORDER BY status ASC
