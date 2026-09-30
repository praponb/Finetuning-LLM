SELECT status, MAX(order_date) AS last_order_date, COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count FROM orders WHERE store_id IS NULL AND channel = 'partner' GROUP BY status;
