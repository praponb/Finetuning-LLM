SELECT quantity
FROM inventory
WHERE quantity <= 217
UNION
SELECT quantity
FROM inventory
WHERE last_restock <= '2024-10-01'
