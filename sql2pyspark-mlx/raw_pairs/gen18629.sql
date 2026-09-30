SELECT
    id,
    plan,
    end_date,
    RANK() OVER (PARTITION BY plan ORDER BY end_date DESC) AS rnk
FROM subscriptions
WHERE MONTH(start_date) IN (3, 6, 7)
