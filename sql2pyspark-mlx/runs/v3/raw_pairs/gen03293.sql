WITH AccountsByCustomer AS (
    SELECT customer_id, SUM(CASE WHEN account_type = 'CREDIT' THEN balance ELSE 0 END) AS credit_balance
    FROM accounts
    WHERE customer_id IN (4, 39)
    GROUP BY customer_id
)
SELECT c.credit_limit, A.credit_balance
FROM customers c
LEFT JOIN AccountsByCustomer A ON c.id = A.customer_id
ORDER BY credit_limit ASC, 2 DESC;
