SELECT * FROM (SELECT id, order_id, amount, method, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS row_num FROM payments) AS ranked WHERE row_num <= 2;
