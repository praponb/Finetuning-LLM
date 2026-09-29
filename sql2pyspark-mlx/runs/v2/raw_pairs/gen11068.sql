SELECT *
FROM (
    SELECT
        product_id,
        CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        AVG(quantity) AS avg_quantity,
        COUNT(DISTINCT quantity) AS unique_orders
    FROM order_items
    GROUP BY product_id, CASE WHEN unit_price >= 360 THEN 'high' WHEN unit_price >= 120 THEN 'mid' ELSE 'low' END
) AS agg
WHERE unique_orders < 5;
