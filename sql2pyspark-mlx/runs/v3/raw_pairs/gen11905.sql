WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE order_date BETWEEN '2023-10-18' AND '2024-01-01'
),
shipping_fee_bucket_summary AS (
    SELECT CASE WHEN shipping_fee >= 33 THEN 'high' WHEN shipping_fee >= 13 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, SUM(discount) AS total_discount
    FROM valid_orders
    GROUP BY CASE WHEN shipping_fee >= 33 THEN 'high' WHEN shipping_fee >= 13 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM shipping_fee_bucket_summary
ORDER BY shipping_fee_bucket DESC
LIMIT 5
