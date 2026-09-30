SELECT t.id, t.merchant
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance BETWEEN 28000 AND 41000)
ORDER BY t.merchant ASC;
