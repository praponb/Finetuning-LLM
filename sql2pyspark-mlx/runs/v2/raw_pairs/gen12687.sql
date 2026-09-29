WITH RecentOrders AS (
    SELECT *
    FROM orders
    WHERE amount <= 1030
),
agg AS (
    SELECT
        status,
        channel,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
    FROM RecentOrders
    GROUP BY status, channel
)
SELECT *
FROM agg
WHERE partner_count > 69
ORDER BY status;
