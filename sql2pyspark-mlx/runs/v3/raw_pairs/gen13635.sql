SELECT a.id, a.balance
FROM accounts a
WHERE a.opened_date < '2023-03-01' AND a.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit >= 11900);
