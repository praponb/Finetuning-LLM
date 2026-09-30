SELECT YEAR(last_restock) AS last_restock_year, DATE_TRUNC('MONTH', last_restock) AS last_restock_month, MIN(last_restock) AS first_last_restock
FROM inventory
WHERE NOT (product_id IN (2, 3, 6, 8) AND product_id IN (1, 3, 4, 8))
GROUP BY last_restock_year, last_restock_month
