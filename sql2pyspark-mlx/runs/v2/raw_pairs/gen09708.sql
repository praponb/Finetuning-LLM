WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE customer_id = 39 AND customer_id = 13 AND MONTH(end_date) IN (5, 7, 8)
),
plan_summary AS (
    SELECT plan, MIN(start_date) AS first_start_date, SUM(monthly_fee) AS total_monthly_fee
    FROM src
    GROUP BY plan
)
SELECT *
FROM plan_summary
ORDER BY first_start_date DESC, plan
LIMIT 5;
