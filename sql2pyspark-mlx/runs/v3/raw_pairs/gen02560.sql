SELECT
    a.id,
    a.account_type,
    a.balance,
    a.customer_id
FROM accounts a
WHERE a.customer_id IS NOT NULL AND a.customer_id IN (SELECT id FROM customers WHERE signup_date < '2023-12-02');
