SELECT
    a.account_type,
    a.customer_id,
    a.status,
    t.account_id
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE a.account_type NOT IN ('SAVINGS', 'CREDIT') AND t.amount <= 2500;
