SELECT store_id, MIN(quantity) min_quantity FROM inventory WHERE store_id IN (1, 2, 4, 5) AND quantity >= 185 GROUP BY store_id HAVING min_quantity >= 80 ORDER BY min_quantity
