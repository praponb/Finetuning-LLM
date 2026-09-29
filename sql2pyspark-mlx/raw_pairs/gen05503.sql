SELECT
    plan,
    status,
    DATE_TRUNC('MONTH', end_date) AS end_month,
    MAX(end_date) AS last_end_date
FROM subscriptions
GROUP BY 1, 2, 3
ORDER BY 4, end_month DESC, plan, status
LIMIT 50
