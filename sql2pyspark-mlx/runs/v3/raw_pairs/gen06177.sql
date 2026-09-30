SELECT
    id,
    merchant,
    amount,
    ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS rn
FROM transactions
WHERE account_id IN (1, 15, 37);
