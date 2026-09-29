SELECT
    a.status,
    a.customer_id,
    a.balance,
    t.amount,
    CONCAT(a.status, ' ', a.account_type) AS status_account_type
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
WHERE a.balance <= 43000
