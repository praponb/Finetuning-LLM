SELECT store_id, product_id, last_restock
FROM inventory
WHERE last_restock <= '2024-04-17' AND last_restock BETWEEN '2023-05-01' AND '2024-07-01'
