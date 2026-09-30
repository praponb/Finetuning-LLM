WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (2, 7, 9)
),
summary AS (
    SELECT
        product_id,
        CASE WHEN units >= 39 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END AS units_bucket,
        MAX(units) AS max_units,
        AVG(units) AS avg_units
    FROM filtered_store_sales
    GROUP BY product_id, CASE WHEN units >= 39 THEN 'high' WHEN units >= 11 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE avg_units >= 33
ORDER BY product_id DESC;
