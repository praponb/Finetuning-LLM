SELECT
    transactions.id,
    transactions.account_id,
    accounts.customer_id,
    customers.age,
    customers.credit_limit
FROM transactions
RIGHT JOIN accounts ON transactions.account_id = accounts.id
RIGHT JOIN customers ON accounts.customer_id = customers.id
