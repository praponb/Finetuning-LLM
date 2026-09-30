WITH ranked AS (
    SELECT
        id,
        segment,
        signup_date,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY signup_date DESC) AS rnk
    FROM customers
)
SELECT id, segment, signup_date
FROM ranked
WHERE rnk <= 2
