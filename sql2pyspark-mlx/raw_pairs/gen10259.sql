WITH ranked AS (
    SELECT id, end_date
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id, status ORDER BY start_date) AS rank_in_group
        FROM subscriptions
    ) t
    WHERE rank_in_group = 1
)
SELECT
    DATE_TRUNC('MONTH', end_date) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    GROUPING_ID() AS gid
FROM ranked
GROUP BY ROLLUP (DATE_TRUNC('MONTH', end_date))
ORDER BY distinct_ids_2 DESC
