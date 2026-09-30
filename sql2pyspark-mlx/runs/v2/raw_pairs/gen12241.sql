WITH Numbered AS (SELECT id, order_id, quantity, product_id, RANK() OVER (PARTITION BY order_id ORDER BY quantity) AS rn FROM order_items) SELECT * FROM Numbered WHERE rn <= 5 ORDER BY id DESC;
