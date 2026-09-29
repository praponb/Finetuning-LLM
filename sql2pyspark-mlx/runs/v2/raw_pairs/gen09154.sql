SELECT *, ROUND(quantity * 1.15, 2) AS adjusted_quantity, ROUND(quantity * 0.9, 2) AS quantity_scaled
FROM inventory
WHERE quantity != 152 OR last_restock >= '2023-09-07'
ORDER BY store_id DESC, id
LIMIT 1;
