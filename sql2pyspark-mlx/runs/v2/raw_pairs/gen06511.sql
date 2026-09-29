WITH customer_totals AS (
    SELECT customer_id, COUNT(DISTINCT country) AS unique_countrys
    FROM sessions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.age,
    c.signup_date,
    ct.unique_countrys
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id
WHERE ct.unique_countrys >= 6
ORDER BY 2 DESC;
