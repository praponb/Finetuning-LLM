SELECT id, quantity, CASE WHEN quantity >= 209 THEN 'High' WHEN quantity >= 75 THEN 'Medium' ELSE 'Low' END quantity_band
FROM inventory
WHERE last_restock BETWEEN '2024-06-01' AND '2024-10-05' AND quantity BETWEEN 106 AND 203
ORDER BY quantity ASC;
