SELECT
    a.id,
    a.balance,
    a.account_type,
    a.status
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE merchant = '7-Eleven');
