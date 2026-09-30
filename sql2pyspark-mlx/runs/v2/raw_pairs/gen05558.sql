SELECT *, FLOOR(quantity) AS quantity_whole, ROUND(quantity, 0) AS quantity_rounded
FROM inventory
WHERE last_restock BETWEEN '2023-12-01' AND '2025-04-01' AND quantity IS NOT NULL AND product_id IN (1, 2, 8)
ORDER BY store_id
