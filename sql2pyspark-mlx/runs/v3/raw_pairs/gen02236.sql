WITH customer_summary AS (
    SELECT customer_id, COUNT(DISTINCT balance) unique_account_types, MAX(balance) max_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT
    c.segment,
    c.signup_date,
    cs.unique_account_types,
    cs.max_balance
FROM customers c
INNER JOIN customer_summary cs ON c.id = cs.customer_id
WHERE cs.unique_account_types > 7
ORDER BY segment DESC
