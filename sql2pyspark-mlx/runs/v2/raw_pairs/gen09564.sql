WITH ranked AS (
    SELECT
        id,
        country,
        signup_date,
        name,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rnk
    FROM customers
)
SELECT
    id,
    country,
    signup_date,
    name
FROM ranked
WHERE rnk <= 3
ORDER BY id
