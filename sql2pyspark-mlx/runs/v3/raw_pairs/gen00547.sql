SELECT
    a.id,
    a.status,
    a.account_type,
    a.balance
FROM accounts a
WHERE a.account_type != 'CHECKING' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant = 'Grab')
ORDER BY account_type ASC, 2 DESC
