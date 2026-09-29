WITH accounts_by_customer AS (
    SELECT customer_id, SUM(balance) total_balance
    FROM accounts
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.signup_date,
    customers.segment,
    accounts_by_customer.total_balance
FROM customers
LEFT JOIN accounts_by_customer ON customers.id = accounts_by_customer.customer_id
ORDER BY signup_date
