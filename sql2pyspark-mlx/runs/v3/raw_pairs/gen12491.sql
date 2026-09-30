SELECT *
FROM (
    SELECT
        id,
        merchant,
        amount,
        DENSE_RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS pos
    FROM transactions
    WHERE amount < 2400
) AS t
WHERE pos = 1
ORDER BY id ASC, merchant ASC;
