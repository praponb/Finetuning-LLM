WITH dim_totals AS (
    SELECT
        d.channel,
        e.method,
        COUNT(*) AS num_rows,
        ROUND(AVG(e.amount), 2) AS avg_amount,
        GROUPING_ID() AS level_id
    FROM payments e
    LEFT JOIN orders d ON e.order_id = d.id
    GROUP BY GROUPING SETS ((d.channel, e.method), (d.channel), ())
)
SELECT
    method,
    avg_amount,
    CASE WHEN method RLIKE '^(paypal|cash)$' THEN 'EXTENDED' WHEN method RLIKE '^(card|bank_transfer)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS method_zone
FROM dim_totals
ORDER BY method NULLS FIRST
