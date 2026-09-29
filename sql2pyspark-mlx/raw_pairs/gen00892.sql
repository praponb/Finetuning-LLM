SELECT *
FROM inventory
WHERE last_restock BETWEEN '2023-06-11' AND '2023-09-01' AND store_id IN (1, 2);
