WITH filtered_subscriptions AS (
    SELECT *, ADD_MONTHS(start_date, 6) AS start_date_plus_months
    FROM subscriptions
    WHERE plan != 'standard' AND (monthly_fee >= 210 OR monthly_fee >= 160)
),
summary AS (
    SELECT status, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, COUNT(*) AS total_count
    FROM filtered_subscriptions
    GROUP BY status
)
SELECT status, stddev_monthly_fee, total_count
FROM summary
ORDER BY stddev_monthly_fee, status
LIMIT 20;
