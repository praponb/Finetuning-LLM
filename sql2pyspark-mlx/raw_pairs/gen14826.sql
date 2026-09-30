WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(start_date) >= 2025
),
summary AS (
    SELECT MONTH(end_date) AS end_month, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM src
    GROUP BY MONTH(end_date)
)
SELECT end_month, stddev_monthly_fee
FROM summary
ORDER BY 1 DESC, stddev_monthly_fee ASC
LIMIT 50;
