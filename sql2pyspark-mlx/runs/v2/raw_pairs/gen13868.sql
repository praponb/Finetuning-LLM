SELECT id, quantity
FROM inventory
WHERE product_id IN (6, 7, 8, 9) AND MONTH(last_restock) IN (4, 10, 11) AND last_restock BETWEEN '2023-03-01' AND '2023-09-01'
