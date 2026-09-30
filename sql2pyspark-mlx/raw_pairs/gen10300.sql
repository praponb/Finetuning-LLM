WITH CustomerSummary AS (
    SELECT customer_id, AVG(balance) AS avg_balance, MAX(balance) AS max_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.name,
    c.credit_limit,
    C.avg_balance,
    C.max_balance
FROM customers c
JOIN CustomerSummary C ON c.id = C.customer_id
WHERE C.max_balance > 52000
