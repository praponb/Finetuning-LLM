WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(start_date) = 2024 AND end_date BETWEEN '2024-05-15' AND '2025-07-24'
),
status_summary AS (
    SELECT status, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee, MIN(monthly_fee) AS min_monthly_fee
    FROM src
    GROUP BY status
)
SELECT status, avg_monthly_fee, min_monthly_fee
FROM status_summary
WHERE avg_monthly_fee > 44
ORDER BY avg_monthly_fee DESC, min_monthly_fee DESC;
