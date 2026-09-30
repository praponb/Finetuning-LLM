WITH ValidOrderItems AS (
    SELECT *
    FROM order_items
    WHERE unit_price >= 180
),
unit_price_bucket_summary AS (
    SELECT CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT quantity) AS unique_products
    FROM ValidOrderItems
    GROUP BY CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 150 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM unit_price_bucket_summary;
