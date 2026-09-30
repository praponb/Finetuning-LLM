SELECT store_id
FROM inventory
WHERE store_id IN (3, 5, 6) OR last_restock BETWEEN '2023-06-07' AND '2023-07-26'
