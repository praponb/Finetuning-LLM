SELECT *
FROM (
    SELECT
        CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        AVG(quantity) AS avg_quantity,
        COUNT(*) AS total_count
    FROM order_items
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 180 THEN 'mid' ELSE 'low' END
) AS s
WHERE avg_quantity > 4.9
ORDER BY total_count, unit_price_bucket, quantity_bucket
LIMIT 10
