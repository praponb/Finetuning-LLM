SELECT
    id,
    merchant,
    amount,
    account_id
FROM (
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS pos
    FROM transactions
    WHERE merchant NOT IN ('Grab', 'Amazon')
) AS t
WHERE pos <= 3;
