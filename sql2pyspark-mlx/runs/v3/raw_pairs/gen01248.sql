WITH accounts_by_customer AS (
    SELECT customer_id, SUM(CASE WHEN status = 'FROZEN' THEN balance ELSE 0 END) AS frozen_balance
    FROM accounts
    WHERE status = 'OPEN'
    GROUP BY customer_id
)
SELECT
    customers.id,
    customers.city,
    customers.segment,
    accounts_by_customer.frozen_balance
FROM customers
INNER JOIN accounts_by_customer ON customers.id = accounts_by_customer.customer_id
ORDER BY customers.id DESC;
