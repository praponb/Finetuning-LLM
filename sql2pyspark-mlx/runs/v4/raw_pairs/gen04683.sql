SELECT last_restock FROM inventory WHERE last_restock < '2025-07-06' UNION ALL SELECT last_restock FROM inventory WHERE product_id = 9 ORDER BY last_restock
