SELECT id, amount
FROM transactions
WHERE account_id IN (4, 29, 34) AND account_id IN (4, 6, 31, 37)
