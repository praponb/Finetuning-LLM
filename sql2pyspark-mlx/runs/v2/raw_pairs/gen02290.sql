SELECT
    status,
    plan,
    MAX(end_date) AS last_end_date,
    MIN(start_date) AS min_start_date
FROM subscriptions
GROUP BY CUBE(status, plan)
