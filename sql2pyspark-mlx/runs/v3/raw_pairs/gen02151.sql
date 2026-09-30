SELECT store_id, unique_products
FROM (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE store_id IN (3, 4, 5, 6)
    GROUP BY store_id
) AS x
WHERE unique_products >= 7
