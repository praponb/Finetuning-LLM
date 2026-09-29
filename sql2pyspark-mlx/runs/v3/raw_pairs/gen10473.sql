SELECT MIN(last_restock) min_last_restock, ROUND(STDDEV(quantity), 2) stddev_quantity
FROM inventory
ORDER BY stddev_quantity DESC
