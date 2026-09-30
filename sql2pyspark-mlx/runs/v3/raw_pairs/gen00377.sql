WITH TopSubscriptions AS (
    SELECT
        id,
        customer_id,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS rnk
    FROM subscriptions
)
SELECT *
FROM TopSubscriptions
WHERE rnk <= 5
