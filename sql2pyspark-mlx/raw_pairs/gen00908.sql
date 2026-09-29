WITH valid_subscriptions AS (
    SELECT *, UPPER(status) AS status_upper
    FROM subscriptions
    WHERE monthly_fee IS NULL OR customer_id IS NOT NULL
),
subscriptions_stats AS (
    SELECT MONTH(start_date) AS start_month, SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count
    FROM valid_subscriptions
    GROUP BY MONTH(start_date)
)
SELECT *
FROM subscriptions_stats
WHERE standard_count >= 78
ORDER BY start_month DESC;
