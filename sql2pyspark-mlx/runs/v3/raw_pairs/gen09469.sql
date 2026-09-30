SELECT *
FROM (
    SELECT
        product_id,
        CASE WHEN units >= 38 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END AS units_bucket,
        MIN(units) AS min_units,
        MAX(units) - MIN(units) AS units_range
    FROM store_sales
    GROUP BY product_id, CASE WHEN units >= 38 THEN 'high' WHEN units >= 20 THEN 'mid' ELSE 'low' END
) AS sub
WHERE min_units > 21
