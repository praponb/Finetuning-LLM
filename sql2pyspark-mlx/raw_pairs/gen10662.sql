SELECT accounts.balance, customers.city
FROM accounts
INNER JOIN customers ON accounts.customer_id = customers.id
ORDER BY customers.city DESC, balance
