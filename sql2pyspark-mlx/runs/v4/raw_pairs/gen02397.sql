WITH top_inventory AS (SELECT id, store_id, quantity, product_id, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY quantity DESC, id) AS rn FROM inventory) SELECT * FROM top_inventory WHERE rn <= 5
