SELECT id, order_id, amount, paid_at FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num FROM payments) AS r WHERE row_num <= 5;
