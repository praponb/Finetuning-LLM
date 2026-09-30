WITH valid_order_items AS (
    SELECT *, FLOOR(unit_price) AS unit_price_whole
    FROM order_items
    WHERE product_id IN (11, 22)
),
summary AS (
    SELECT CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MAX(unit_price) - MIN(unit_price) AS unit_price_range
    FROM valid_order_items
    GROUP BY CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, unit_price_range
FROM summary
WHERE unit_price_range > 28
ORDER BY unit_price_range;
