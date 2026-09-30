SELECT id, order_id, quantity FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS row_num FROM order_items) AS ranked WHERE row_num <= 2
