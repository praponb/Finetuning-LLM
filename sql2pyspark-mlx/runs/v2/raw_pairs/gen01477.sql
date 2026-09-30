SELECT id, status, order_id FROM payments WHERE method != 'cash' UNION ALL SELECT id, status, order_id FROM payments WHERE status = 'REFUNDED' ORDER BY status;
