SELECT
    a.id,
    a.opened_date,
    a.status,
    a.account_type
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant = 'Shell');
