SELECT last_restock FROM inventory WHERE store_id IN (1, 3, 5, 6) UNION ALL SELECT delivered_date FROM shipments ORDER BY last_restock;
