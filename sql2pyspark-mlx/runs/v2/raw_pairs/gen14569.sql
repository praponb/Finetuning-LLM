SELECT COUNT(quantity) AS quantity_count, ROUND(STDDEV(quantity), 2) AS stddev_quantity FROM inventory WHERE store_id IN (2, 3, 5, 6) ORDER BY quantity_count DESC;
