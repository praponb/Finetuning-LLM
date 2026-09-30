WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rnk
    FROM subscriptions
)
SELECT *
FROM ranked
WHERE rnk <= 3;
