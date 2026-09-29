WITH recent_subscriptions AS (
    SELECT *, INITCAP(TRIM(status)) status_title
    FROM subscriptions
    WHERE (start_date BETWEEN '2024-04-16' AND '2025-10-01' AND monthly_fee <= 90) OR start_date > '2023-11-30'
),
summary AS (
    SELECT DATE_FORMAT(end_date, 'yyyy-MM') end_month, AVG(monthly_fee) avg_monthly_fee
    FROM recent_subscriptions
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT end_month, avg_monthly_fee
FROM summary
WHERE avg_monthly_fee < 160
