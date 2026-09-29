SELECT last_restock, quantity
FROM inventory
WHERE last_restock > '2023-11-01'
EXCEPT
SELECT last_restock, quantity
FROM inventory
WHERE YEAR(last_restock) = 2023;
