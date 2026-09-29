SELECT account_id, merchant
FROM transactions
WHERE account_id IN (8, 9, 18, 20)
UNION
SELECT account_id, merchant
FROM transactions
WHERE is_flagged = TRUE
