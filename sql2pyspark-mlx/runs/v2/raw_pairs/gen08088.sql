WITH filtered_subscriptions AS (
    SELECT *, ROUND(monthly_fee * 1.15, 2) AS adjusted_monthly_fee
    FROM subscriptions
    WHERE plan IN ('basic', 'standard', 'premium') AND end_date <= '2024-03-01'
),
agg AS (
    SELECT QUARTER(start_date) AS start_quarter, SUM(adjusted_monthly_fee) AS total_adjusted_monthly_fee, ROUND(AVG(adjusted_monthly_fee), 2) AS avg_adjusted_monthly_fee
    FROM filtered_subscriptions
    GROUP BY QUARTER(start_date)
)
SELECT start_quarter, total_adjusted_monthly_fee, avg_adjusted_monthly_fee
FROM agg
WHERE total_adjusted_monthly_fee > 210
ORDER BY total_adjusted_monthly_fee DESC
