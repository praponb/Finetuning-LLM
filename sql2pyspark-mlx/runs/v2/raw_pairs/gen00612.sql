SELECT accounts.id, accounts.status
FROM accounts
WHERE accounts.customer_id NOT IN (SELECT id FROM customers WHERE credit_limit IS NULL)
ORDER BY accounts.id DESC;
