SELECT id, product_id, quantity FROM (SELECT id, product_id, quantity, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rn FROM order_items) AS r WHERE rn <= 2 ORDER BY id DESC
