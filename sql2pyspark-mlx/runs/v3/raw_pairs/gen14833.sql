SELECT quantity FROM inventory WHERE MONTH(last_restock) IN (3, 5, 9) UNION ALL SELECT quantity FROM inventory WHERE store_id IN (2, 6)
