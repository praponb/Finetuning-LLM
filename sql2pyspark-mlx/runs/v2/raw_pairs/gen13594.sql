WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date, id) AS pos
    FROM subscriptions
    WHERE monthly_fee > 200
)
SELECT
    id,
    customer_id,
    end_date,
    plan
FROM ranked
WHERE pos <= 3
