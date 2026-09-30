WITH FilteredSubscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(end_date) = 2024
),
subscriptions_stats AS (
    SELECT QUARTER(start_date) AS start_quarter, COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) AS expired_count
    FROM FilteredSubscriptions
    GROUP BY QUARTER(start_date)
)
SELECT *
FROM subscriptions_stats
