SELECT product_id, min_units
FROM (
    SELECT product_id, MIN(units) AS min_units
    FROM store_sales
    GROUP BY product_id
) AS s
WHERE min_units >= 24;
