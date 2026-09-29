WITH base AS (
    SELECT *
    FROM subscriptions
    WHERE (monthly_fee IS NOT NULL AND YEAR(start_date) = 2023) OR customer_id = 28
),
summary AS (
    SELECT QUARTER(end_date) AS end_quarter, MAX(end_date) AS latest_end_date
    FROM base
    GROUP BY QUARTER(end_date)
)
SELECT end_quarter, latest_end_date
FROM summary
ORDER BY end_quarter DESC, latest_end_date
