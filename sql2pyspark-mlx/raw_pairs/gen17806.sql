SELECT customers.name, t.avg_balance, t.total_balance
FROM customers
INNER JOIN (SELECT customer_id, AVG(balance) AS avg_balance, SUM(balance) AS total_balance
    FROM accounts
    GROUP BY customer_id) t ON customers.id = t.customer_id
WHERE t.avg_balance > 25000
ORDER BY t.total_balance DESC;
