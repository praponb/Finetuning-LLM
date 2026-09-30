SELECT DATE_TRUNC('MONTH', last_restock) AS last_restock_month, ROUND(AVG(quantity), 2) AS avg_quantity, MAX(quantity) AS max_quantity
FROM inventory
WHERE quantity >= 70 AND product_id = 5
GROUP BY DATE_TRUNC('MONTH', last_restock)
ORDER BY last_restock_month DESC
LIMIT 10;
