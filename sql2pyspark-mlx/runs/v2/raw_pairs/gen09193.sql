SELECT
    accounts.id,
    accounts.opened_date,
    accounts.customer_id,
    accounts.balance
FROM accounts
WHERE accounts.customer_id IN (SELECT id FROM customers WHERE signup_date <= '2024-12-16')
ORDER BY 2, balance DESC;
