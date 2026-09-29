SELECT
    product_id,
    unit_price_bucket,
    avg_unit_price,
    min_unit_price
FROM (
    SELECT
        product_id,
        CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        AVG(unit_price) AS avg_unit_price,
        MIN(unit_price) AS min_unit_price
    FROM order_items
    GROUP BY product_id, CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 70 THEN 'mid' ELSE 'low' END
) AS t
WHERE min_unit_price < 170
ORDER BY min_unit_price DESC, unit_price_bucket;
