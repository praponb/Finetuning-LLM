SELECT store_id, avg_quantity, max_quantity
FROM (
    SELECT store_id, AVG(quantity) AS avg_quantity, MAX(quantity) AS max_quantity
    FROM inventory
    WHERE product_id IN (1, 9)
    GROUP BY store_id
) AS s
WHERE avg_quantity > 240
ORDER BY store_id DESC
LIMIT 20
