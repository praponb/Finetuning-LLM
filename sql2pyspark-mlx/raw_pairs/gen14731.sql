WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS pos FROM inventory) SELECT id, product_id, last_restock, store_id FROM ranked WHERE pos = 1
