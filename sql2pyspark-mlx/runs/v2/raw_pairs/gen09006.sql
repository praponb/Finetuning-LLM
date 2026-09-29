SELECT store_id, product_id, MIN(quantity) AS min_quantity
FROM inventory
GROUP BY store_id, product_id
HAVING MIN(quantity) < 110;
