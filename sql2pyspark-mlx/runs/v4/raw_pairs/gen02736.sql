SELECT
    id,
    method,
    amount,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) row_num
    FROM payments
) r
WHERE row_num <= 2;
