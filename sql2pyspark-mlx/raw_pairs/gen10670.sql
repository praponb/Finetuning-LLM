SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MIN(quantity) AS min_quantity
FROM inventory
WHERE last_restock > '2024-08-03' OR store_id = 6
GROUP BY last_restock_month
