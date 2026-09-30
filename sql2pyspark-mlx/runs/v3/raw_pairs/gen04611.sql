WITH ValidSubscriptions AS (
    SELECT *, SUBSTRING(status, 1, 3) AS status_prefix
    FROM subscriptions
    WHERE monthly_fee < 190
),
subscriptions_stats AS (
    SELECT MONTH(start_date) AS start_month, SORT_ARRAY(COLLECT_SET(status)) AS status_list
    FROM ValidSubscriptions
    GROUP BY MONTH(start_date)
)
SELECT start_month, status_list
FROM subscriptions_stats;
