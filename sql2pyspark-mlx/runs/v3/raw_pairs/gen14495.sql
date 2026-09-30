WITH filtered_orders AS (
    SELECT *, CONCAT(channel, ' - ', status) AS channel_status
    FROM orders
    WHERE discount > 0.33
),
agg AS (
    SELECT status, CASE WHEN status IN ('PENDING', 'RETURNED') THEN 'core' ELSE 'other' END AS status_group, SUM(CASE WHEN status = 'SHIPPED' THEN amount ELSE 0 END) AS shipped_amount
    FROM filtered_orders
    GROUP BY status, CASE WHEN status IN ('PENDING', 'RETURNED') THEN 'core' ELSE 'other' END
)
SELECT status, status_group, shipped_amount
FROM agg
ORDER BY status_group;
