SELECT id, order_id, method, amount, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount DESC, id) AS row_num FROM payments
