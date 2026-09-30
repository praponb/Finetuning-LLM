SELECT store_id, MIN(last_restock) min_last_restock, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM inventory
WHERE NOT (store_id IN (1, 5) AND product_id IN (4, 7, 9, 10) AND product_id IN (3, 6, 9, 10))
GROUP BY store_id
