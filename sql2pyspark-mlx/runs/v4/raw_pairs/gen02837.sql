SELECT id, product_id FROM inventory WHERE quantity = 86 AND (YEAR(last_restock) = 2023 OR store_id IN (4, 5)) ORDER BY product_id, id LIMIT 10
