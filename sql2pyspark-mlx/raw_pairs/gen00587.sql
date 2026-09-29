WITH ranked AS (SELECT * FROM inventory QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST, id) = 1) SELECT id, store_id, product_id, quantity, last_restock FROM ranked;
