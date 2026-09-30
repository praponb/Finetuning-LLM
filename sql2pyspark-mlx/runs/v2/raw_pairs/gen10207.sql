SELECT id, order_id, amount, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS row_num FROM payments ORDER BY id DESC
