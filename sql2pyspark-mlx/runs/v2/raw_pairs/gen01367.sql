WITH accounts_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT c.credit_limit, abc.closed_balance
FROM customers c
JOIN accounts_by_customer abc ON c.id = abc.customer_id
ORDER BY credit_limit DESC;
