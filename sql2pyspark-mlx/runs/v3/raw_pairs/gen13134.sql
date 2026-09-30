SELECT accounts.id, accounts.account_type, customers.segment
FROM accounts
JOIN customers ON customers.id = accounts.customer_id
WHERE customers.city IN ('Berlin', 'Tokyo') AND accounts.customer_id = 33
ORDER BY 2;
