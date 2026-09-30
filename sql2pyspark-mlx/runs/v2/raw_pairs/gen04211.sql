WITH AccountsByCustomer AS (
    SELECT customer_id, AVG(balance) AS avg_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT c.segment, c.is_active, A.avg_balance
FROM customers c
LEFT JOIN AccountsByCustomer A ON c.id = A.customer_id
ORDER BY c.segment DESC
