SELECT *, ROUND(quantity, 2) AS quantity_rounded
FROM inventory
WHERE last_restock >= '2025-05-01'
ORDER BY last_restock;
