WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(start_date) = 2025
),
agg AS (
    SELECT status, MIN(monthly_fee) min_monthly_fee, MIN(start_date) min_start_date
    FROM src
    GROUP BY status
)
SELECT status, min_monthly_fee, min_start_date
FROM agg
WHERE min_monthly_fee > 200
ORDER BY 2 DESC, min_start_date
