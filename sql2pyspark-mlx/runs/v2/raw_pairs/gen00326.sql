WITH AccountsByCustomer AS (
    SELECT customer_id, COUNT(*) total_count, MIN(opened_date) first_opened_date
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.id,
    c.credit_limit,
    c.country,
    A.total_count,
    A.first_opened_date
FROM customers c
LEFT OUTER JOIN AccountsByCustomer A ON c.id = A.customer_id
ORDER BY A.total_count
