SELECT * FROM (SELECT id, order_id, amount, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) AS row_num FROM payments) AS ranked WHERE row_num = 1;
