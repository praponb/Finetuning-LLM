SELECT *
FROM (
    SELECT store_id, COUNT(DISTINCT units) AS unique_products, MAX(units) AS max_units
    FROM store_sales
    WHERE revenue >= 3000
    GROUP BY store_id
) AS t
WHERE unique_products > 7
ORDER BY unique_products DESC, store_id
LIMIT 5;
