SELECT *
FROM (
    SELECT store_id, SUM(quantity) AS total_quantity, AVG(quantity) AS avg_quantity
    FROM inventory
    WHERE store_id IN (3, 5, 6)
    GROUP BY store_id
) AS s
WHERE total_quantity > 970;
