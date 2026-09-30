SELECT customers.name, t.max_balance
FROM customers
JOIN (SELECT customer_id, MAX(balance) AS max_balance
    FROM accounts
    GROUP BY customer_id) t ON customers.id = t.customer_id
WHERE t.max_balance > 52000
