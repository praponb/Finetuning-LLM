SELECT *
FROM inventory
WHERE quantity >= 84 AND (product_id = 1 OR last_restock >= '2023-04-03')
ORDER BY last_restock ASC
