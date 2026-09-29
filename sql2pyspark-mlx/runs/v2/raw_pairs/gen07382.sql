SELECT *
FROM (
    SELECT
        id,
        merchant,
        amount,
        is_flagged,
        RANK() OVER (PARTITION BY merchant ORDER BY amount DESC) AS rn
    FROM transactions
) AS ranked
WHERE rn <= 5
ORDER BY rn;
