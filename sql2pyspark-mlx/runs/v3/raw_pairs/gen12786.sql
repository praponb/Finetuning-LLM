SELECT
    id,
    merchant,
    amount,
    is_flagged
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS pos
    FROM transactions
) AS r
WHERE pos <= 1
ORDER BY amount DESC;
