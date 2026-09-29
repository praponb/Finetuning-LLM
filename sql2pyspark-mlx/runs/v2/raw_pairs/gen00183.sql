SELECT account_id
FROM transactions
WHERE merchant = 'Grab'
UNION
SELECT account_id
FROM transactions
WHERE account_id IN (4, 32)
ORDER BY account_id;
