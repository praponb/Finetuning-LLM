SELECT store_id, product_id, YEAR(last_restock) AS last_restock_year
FROM inventory
WHERE quantity BETWEEN 75 AND 225 OR product_id IN (1, 9)
ORDER BY store_id DESC
