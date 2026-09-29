SELECT product_id, COUNT(quantity) AS quantity_count
FROM inventory
WHERE product_id IN (1, 7, 9) OR store_id = 4
GROUP BY product_id
HAVING MAX(quantity) > 170;
