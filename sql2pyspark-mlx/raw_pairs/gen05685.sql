SELECT id, product_id FROM inventory QUALIFY RANK() OVER (PARTITION BY product_id, store_id ORDER BY last_restock NULLS FIRST) = 1
