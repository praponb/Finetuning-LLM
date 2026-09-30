WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (17, 33, 37, 40)
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
    DATE_TRUNC('WEEK', order_date) AS week_start,
    DATE_TRUNC('YEAR', order_date) AS year_start,
    MAX(amount) FILTER (WHERE status = 'SHIPPED') AS shipped_max_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    RANK() OVER (PARTITION BY CASE WHEN shipping_fee >= 41 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY STDDEV_SAMP(shipping_fee) DESC) AS shipping_fee_stddev_rank
FROM not_flagged
GROUP BY shipping_fee_tier, week_start, year_start
