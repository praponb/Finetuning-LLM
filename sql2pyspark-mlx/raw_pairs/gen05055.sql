SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS row_num FROM order_items) AS r WHERE row_num <= 1;
