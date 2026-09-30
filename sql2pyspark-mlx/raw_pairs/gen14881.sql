WITH top_inventory AS (SELECT id, product_id, quantity, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rn FROM inventory) SELECT * FROM top_inventory WHERE rn <= 5;
