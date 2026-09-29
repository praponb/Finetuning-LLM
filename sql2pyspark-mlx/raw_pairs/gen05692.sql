SELECT
    start_year,
    start_month,
    avg_monthly_fee,
    min_monthly_fee
FROM (
    SELECT
        YEAR(start_date) AS start_year,
        MONTH(start_date) AS start_month,
        AVG(monthly_fee) AS avg_monthly_fee,
        MIN(monthly_fee) AS min_monthly_fee
    FROM subscriptions
    GROUP BY YEAR(start_date), MONTH(start_date)
) AS sub
WHERE min_monthly_fee > 70
ORDER BY start_year DESC
