WITH dim_totals AS (
    SELECT
        d.status AS order_status,
        d.channel,
        COUNT(*) AS num_rows,
        MIN(src.cost) AS min_cost
    FROM shipments src
    LEFT JOIN orders d ON src.order_id = d.id
    GROUP BY GROUPING SETS ((d.status, d.channel), (d.status), ())
)
SELECT
    channel,
    min_cost,
    CASE WHEN channel RLIKE '^(mobile|web)$' THEN 'GROUP_A' WHEN channel RLIKE '^(store|partner)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS channel_zone
FROM dim_totals
ORDER BY channel_zone DESC;
