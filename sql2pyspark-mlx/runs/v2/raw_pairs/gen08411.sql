SELECT product_id, MAX(quantity) - MIN(quantity) AS quantity_range
FROM inventory
WHERE last_restock BETWEEN '2023-07-01' AND '2024-01-27'
GROUP BY product_id
ORDER BY quantity_range ASC, 1 DESC
LIMIT 10;
