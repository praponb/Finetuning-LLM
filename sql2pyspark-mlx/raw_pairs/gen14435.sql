WITH long_format AS (
    SELECT *
    FROM (
        SELECT
            id,
            customer_id,
            discount,
            amount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT INCLUDE NULLS (
        metric_value FOR measure IN (discount, amount, shipping_fee)
    )
)
SELECT
    measure,
    CASE WHEN metric_value >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END AS metric_value_tier,
    COUNT_IF(measure = 'discount') AS discount_events,
    APPROX_PERCENTILE(metric_value, 0.5) AS metric_value_p50
FROM long_format
GROUP BY measure, CASE WHEN metric_value >= 79 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING metric_value_p50 > 0
ORDER BY metric_value_p50 NULLS FIRST
