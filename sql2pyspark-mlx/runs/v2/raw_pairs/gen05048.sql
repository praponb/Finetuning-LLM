SELECT quantity, DATE_ADD(last_restock, 84) AS last_restock_plus_84d
FROM inventory
WHERE (quantity BETWEEN 80 AND 228 AND last_restock >= '2024-03-19') OR quantity BETWEEN 113 AND 178;
