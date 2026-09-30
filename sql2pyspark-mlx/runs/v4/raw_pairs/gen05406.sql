SELECT
    transactions.is_flagged,
    accounts.balance,
    accounts.account_type,
    customers.city,
    FLOOR(customers.age) AS age_whole
FROM transactions
LEFT JOIN accounts ON transactions.account_id = accounts.id
LEFT JOIN customers ON customers.id = accounts.customer_id
ORDER BY age_whole
