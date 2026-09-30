SELECT
    transactions.id,
    transactions.amount,
    transactions.merchant,
    accounts.customer_id,
    accounts.balance,
    customers.name,
    customers.is_active
FROM transactions
JOIN accounts ON transactions.account_id = accounts.id
JOIN customers ON accounts.customer_id = customers.id;
