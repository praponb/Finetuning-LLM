SELECT
    unit_price_bucket,
    product_id,
    unit_price_range,
    max_unit_price
FROM (
    SELECT
        CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        product_id,
        MAX(unit_price) - MIN(unit_price) AS unit_price_range,
        MAX(unit_price) AS max_unit_price
    FROM order_items
    WHERE quantity < 4
    GROUP BY CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 100 THEN 'mid' ELSE 'low' END, product_id
) AS agg
WHERE max_unit_price < 300
ORDER BY unit_price_bucket, product_id
LIMIT 5;
