SELECT id, quantity, ROUND(quantity * 1.07, 2) AS adjusted_quantity
FROM inventory
WHERE last_restock BETWEEN '2025-05-01' AND '2025-06-29' AND product_id IN (6, 8)
