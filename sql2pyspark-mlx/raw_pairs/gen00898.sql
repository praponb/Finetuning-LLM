WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IS NOT NULL
),
status_group_summary AS (
    SELECT CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 'core' ELSE 'other' END status_group, channel, ROUND(STDDEV(amount), 2) stddev_amount
    FROM valid_orders
    GROUP BY CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 'core' ELSE 'other' END, channel
)
SELECT status_group, stddev_amount
FROM status_group_summary;
