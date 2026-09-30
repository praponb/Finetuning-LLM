SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, COUNT(quantity) AS quantity_count
FROM inventory
GROUP BY DATE_TRUNC('MONTH', last_restock)
HAVING quantity_count > 1
ORDER BY quantity_count DESC
