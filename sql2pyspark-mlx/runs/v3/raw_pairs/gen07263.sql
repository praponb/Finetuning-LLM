SELECT *, ADD_MONTHS(last_restock, 1) AS last_restock_plus_months
FROM inventory
WHERE MONTH(last_restock) IN (2, 3, 8) OR product_id IN (1, 2, 4, 7)
ORDER BY last_restock_plus_months DESC, product_id, id
LIMIT 3
