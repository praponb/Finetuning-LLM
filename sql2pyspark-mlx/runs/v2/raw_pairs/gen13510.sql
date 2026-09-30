WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) AS rn
    FROM customers
    WHERE country != 'UK'
)
SELECT id, city, signup_date
FROM ranked
WHERE rn <= 1
