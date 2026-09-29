SELECT MIN(last_restock) AS min_last_restock, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
ORDER BY quantity_range DESC;
