WITH ranked AS (
    SELECT
        id,
        customer_id,
        end_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS pos
    FROM subscriptions
    WHERE monthly_fee >= 210
)
SELECT *
FROM ranked
WHERE pos <= 5
ORDER BY end_date DESC
