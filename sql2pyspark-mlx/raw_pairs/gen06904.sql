SELECT store_id, COUNT(quantity) quantity_count
FROM inventory
WHERE product_id IN (2, 3, 7)
GROUP BY store_id
ORDER BY store_id DESC
