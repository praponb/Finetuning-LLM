WITH valid_subscriptions AS (
    SELECT *, TRIM(status) clean_status
    FROM subscriptions
    WHERE customer_id IN (14, 26, 36, 39)
),
subscriptions_stats AS (
    SELECT DATE_TRUNC('MONTH', start_date) start_month, SUM(monthly_fee) total_monthly_fee
    FROM valid_subscriptions
    GROUP BY DATE_TRUNC('MONTH', start_date)
)
SELECT *
FROM subscriptions_stats;
