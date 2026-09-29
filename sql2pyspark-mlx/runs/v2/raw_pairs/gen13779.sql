WITH ranked_customers AS (
    SELECT
        id,
        country,
        signup_date,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
    FROM customers
)
SELECT id, country, signup_date
FROM ranked_customers
WHERE row_num <= 3
ORDER BY id DESC;
