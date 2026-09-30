SELECT *
FROM (
    SELECT store_id, COUNT(DISTINCT units) AS unique_stores
    FROM store_sales
    WHERE product_id = 9
    GROUP BY store_id
) AS s
WHERE unique_stores > 8
ORDER BY unique_stores;
