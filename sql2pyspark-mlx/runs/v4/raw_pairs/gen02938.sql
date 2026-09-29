SELECT id, order_id, paid_at, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS row_num FROM payments WHERE order_id IS NOT NULL;
