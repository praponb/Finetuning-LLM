WITH filtered_order_items AS (
    SELECT *
    FROM order_items
    WHERE quantity < 4 AND product_id IS NULL
),
quantity_bucket_summary AS (
    SELECT
        CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        product_id,
        SUM(unit_price) AS total_unit_price,
        ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM filtered_order_items
    GROUP BY CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END, product_id
)
SELECT *
FROM quantity_bucket_summary
ORDER BY total_unit_price DESC, quantity_bucket, product_id
LIMIT 50;
