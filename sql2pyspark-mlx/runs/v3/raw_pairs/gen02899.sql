WITH filtered_order_items AS (
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE product_id IN (2, 4, 35)
),
summary AS (
    SELECT CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END AS unit_price_bucket, MIN(quantity) AS min_quantity, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM filtered_order_items
    GROUP BY CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 140 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, min_quantity, avg_unit_price
FROM summary
WHERE min_quantity > 2.8
ORDER BY unit_price_bucket
LIMIT 50;
