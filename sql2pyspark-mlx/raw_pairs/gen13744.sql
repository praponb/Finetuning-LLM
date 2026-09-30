WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE order_id IN (3, 11) AND (unit_price BETWEEN 210 AND 270 OR order_id IN (27, 29, 38))
),
unit_price_bucket_summary AS (
    SELECT CASE WHEN unit_price >= 440 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(quantity) AS min_quantity, MAX(quantity) AS max_quantity
    FROM recent_order_items
    GROUP BY CASE WHEN unit_price >= 440 THEN 'high' WHEN unit_price >= 130 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, min_quantity, max_quantity
FROM unit_price_bucket_summary
ORDER BY min_quantity DESC, unit_price_bucket ASC
LIMIT 20
