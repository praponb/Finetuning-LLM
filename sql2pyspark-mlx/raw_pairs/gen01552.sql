WITH unpivoted AS (
    -- Native SQL UNPIVOT (wide to long)
    SELECT *
    FROM (
        SELECT
            id,
            status,
            discount,
            amount,
            shipping_fee
        FROM orders
    ) src
    UNPIVOT INCLUDE NULLS (
        val FOR metric_name IN (discount AS DISCOUNT_VAL, amount AS AMOUNT, shipping_fee)
    ) unp
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN val >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    status,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(DISTINCT status) AS distinct_status_2,
    COUNT_IF(metric_name = 'DISCOUNT_VAL') AS discount_val_events,
    COUNT_IF(metric_name = 'DISCOUNT_VAL') AS discount_val_events_2,
    GROUPING(CASE WHEN val >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_id
FROM unpivoted
GROUP BY GROUPING SETS ((CASE WHEN val >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END, status), (CASE WHEN val >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
