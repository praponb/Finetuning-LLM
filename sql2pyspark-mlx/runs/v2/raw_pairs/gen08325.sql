WITH CustomerTotals AS (
    SELECT customer_id, COUNT(balance) balance_count
    FROM accounts
    WHERE status IN ('CLOSED', 'OPEN')
    GROUP BY customer_id
)
SELECT c.name, c.country, C.balance_count
FROM customers c
LEFT JOIN CustomerTotals C ON c.id = C.customer_id
ORDER BY name DESC, balance_count DESC
