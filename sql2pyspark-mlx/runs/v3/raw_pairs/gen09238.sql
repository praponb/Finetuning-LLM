WITH ranked_customers AS (
    SELECT
        id,
        city,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) rn
    FROM customers
    WHERE city IN ('Chiang Mai', 'Berlin')
)
SELECT id, city, signup_date
FROM ranked_customers
WHERE rn <= 2
ORDER BY id DESC, 3;
