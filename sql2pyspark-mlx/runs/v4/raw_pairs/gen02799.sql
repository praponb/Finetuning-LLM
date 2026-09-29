SELECT COUNT(DISTINCT quantity) unique_stores, SUM(quantity) total_quantity FROM inventory WHERE store_id IN (2, 3, 6) ORDER BY unique_stores DESC
