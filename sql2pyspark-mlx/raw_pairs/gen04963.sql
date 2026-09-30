SELECT
    QUARTER(start_date) AS start_quarter,
    MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range,
    COUNT(*) AS cnt,
    MIN(start_date) AS first_start_date
FROM subscriptions
GROUP BY 1
HAVING cnt >= 1
ORDER BY start_quarter, first_start_date DESC
