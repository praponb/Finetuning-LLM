WITH base AS (
    SELECT *, DATE_TRUNC('MONTH', start_date) AS start_date_period
    FROM subscriptions
    WHERE customer_id IS NULL AND start_date BETWEEN '2023-06-01' AND '2024-11-01'
),
summary AS (
    SELECT DATE_FORMAT(end_date, 'yyyy-MM') AS end_month, SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM base
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT *
FROM summary
ORDER BY monthly_fee_range DESC;
