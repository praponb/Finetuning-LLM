SELECT last_restock FROM inventory WHERE product_id = 6 UNION SELECT shipped_date FROM shipments ORDER BY last_restock;
