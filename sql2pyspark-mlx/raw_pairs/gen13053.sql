WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.channel,
        s.method,
        COUNT(*) AS cnt,
        MIN(s.amount) AS min_amount
    FROM payments s
    LEFT JOIN orders d ON s.order_id = d.id
    GROUP BY d.channel, s.method
)
SELECT
    channel,
    method,
    min_amount,
    CASE WHEN channel RLIKE '^(store|mobile)$' THEN 'CORE' WHEN channel RLIKE '^(web|partner)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS channel_zone
FROM dim_totals
ORDER BY method DESC NULLS FIRST;
