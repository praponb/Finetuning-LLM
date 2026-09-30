WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        p.status,
        p.channel,
        COUNT(*) AS num_rows,
        SUM(s.quantity) AS total_quantity,
        MIN(s.unit_price) AS min_unit_price
    FROM order_items s
    INNER JOIN orders p ON s.order_id = p.id
    GROUP BY CUBE (p.status, p.channel)
)
SELECT
    channel,
    total_quantity,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'GROUP_A' WHEN channel RLIKE '^(web|mobile)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS channel_zone
FROM dim_totals
ORDER BY channel_zone DESC
