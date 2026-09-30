SELECT product_id, min_units
FROM (
    SELECT product_id, MIN(units) AS min_units
    FROM store_sales
    WHERE store_id IN (2, 3, 4, 5)
    GROUP BY product_id
) AS sub
WHERE min_units < 12
ORDER BY min_units DESC;
