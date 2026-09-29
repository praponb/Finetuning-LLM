SELECT accounts.id, accounts.account_type
FROM accounts
WHERE accounts.customer_id IN (SELECT id FROM customers WHERE name IS NULL)
ORDER BY account_type ASC;
