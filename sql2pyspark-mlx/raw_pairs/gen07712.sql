SELECT start_month, active_count
FROM (
    SELECT DATE_TRUNC('MONTH', start_date) AS start_month, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count
    FROM subscriptions
    GROUP BY DATE_TRUNC('MONTH', start_date)
) AS x
WHERE active_count >= 52
ORDER BY active_count
