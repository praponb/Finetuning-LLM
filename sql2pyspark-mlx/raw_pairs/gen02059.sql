WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE customer_id IN (9, 35)
),
subscriptions_stats AS (
    SELECT CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END AS start_cohort, COUNT(monthly_fee) AS monthly_fee_count
    FROM filtered_subscriptions
    GROUP BY CASE WHEN YEAR(start_date) < 2024 THEN 'before_2024' WHEN YEAR(start_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT start_cohort, monthly_fee_count
FROM subscriptions_stats
WHERE monthly_fee_count > 6
ORDER BY start_cohort ASC;
