SELECT id, quantity, CAST(quantity AS DOUBLE) AS quantity_num
FROM inventory
WHERE MONTH(last_restock) IN (6, 7, 9) AND MONTH(last_restock) IN (1, 5, 10)
ORDER BY id DESC
