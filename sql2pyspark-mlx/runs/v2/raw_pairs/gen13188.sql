SELECT *
FROM (
    SELECT plan, ROUND((100 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cancelled, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM subscriptions
    GROUP BY plan
) AS x
WHERE pct_cancelled > 48
ORDER BY pct_cancelled DESC, plan
LIMIT 5
