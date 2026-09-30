SELECT
    status,
    MONTH(start_date) AS start_month,
    SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) AS expired_count,
    SUM(CASE WHEN plan = 'basic' THEN monthly_fee ELSE 0 END) AS basic_monthly_fee,
    MAX(end_date) AS last_end_date
FROM subscriptions
GROUP BY 1, 2
ORDER BY basic_monthly_fee
