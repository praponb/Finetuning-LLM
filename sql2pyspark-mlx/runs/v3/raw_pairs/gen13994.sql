WITH valid_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE end_date < '2025-01-01' OR end_date <= '2024-09-01'
),
agg AS (
    SELECT plan, QUARTER(end_date) AS end_quarter, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
    FROM valid_subscriptions
    GROUP BY plan, QUARTER(end_date)
)
SELECT plan, end_quarter, stddev_monthly_fee
FROM agg
WHERE stddev_monthly_fee > 42
ORDER BY end_quarter, stddev_monthly_fee DESC
