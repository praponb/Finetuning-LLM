SELECT
    status,
    MAX(end_date) AS last_end_date,
    COUNT(*) AS row_count,
    COUNT(*) AS subscription_count
FROM subscriptions
GROUP BY 1
HAVING row_count > 1
