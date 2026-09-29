WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        COALESCE(discount, 0) AS discount_filled
    FROM orders
    WHERE order_date >= TO_DATE('2023-08-03')
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    STDDEV_SAMP(discount) AS discount_stddev,
    STDDEV_SAMP(discount) AS discount_stddev_2,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    PERCENTILE_APPROX(shipping_fee, 0.95) AS shipping_fee_p95
FROM base
GROUP BY ROLLUP (DATE_TRUNC('WEEK', order_date), DATE_TRUNC('MONTH', order_date))
HAVING PERCENTILE_APPROX(shipping_fee, 0.95) > 0
ORDER BY store_rows
