SELECT id, method, paid_at
FROM (
    SELECT
        id,
        method,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS row_num
    FROM payments
    WHERE paid_at >= '2023-03-15'
) AS r
WHERE row_num <= 5
ORDER BY id ASC;
