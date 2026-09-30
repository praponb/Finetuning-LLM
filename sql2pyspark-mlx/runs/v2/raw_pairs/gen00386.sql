WITH src AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE customer_id IN (27, 37, 38)
),
summary AS (
    SELECT MONTH(end_date) AS end_month, plan, MAX(days_since_end_date) AS max_days_since_end_date
    FROM src
    GROUP BY MONTH(end_date), plan
)
SELECT end_month, plan, max_days_since_end_date
FROM summary
ORDER BY end_month DESC
