WITH filtered_orders AS (
    SELECT *
    FROM orders
    WHERE amount < 420
),
summary AS (
    SELECT
        status,
        CASE WHEN shipping_fee >= 44 THEN 'high' WHEN shipping_fee >= 8 THEN 'mid' ELSE 'low' END shipping_fee_bucket,
        AVG(discount) avg_discount,
        COUNT(*) order_count
    FROM filtered_orders
    GROUP BY status, CASE WHEN shipping_fee >= 44 THEN 'high' WHEN shipping_fee >= 8 THEN 'mid' ELSE 'low' END
)
SELECT status, avg_discount, order_count
FROM summary
WHERE order_count < 5;
