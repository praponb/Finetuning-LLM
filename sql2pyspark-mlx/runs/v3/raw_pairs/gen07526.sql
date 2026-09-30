SELECT
    a.status,
    a.account_type,
    t.merchant,
    t.amount
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE a.status NOT IN ('CLOSED', 'OPEN') AND a.account_type = 'CHECKING'
ORDER BY account_type
