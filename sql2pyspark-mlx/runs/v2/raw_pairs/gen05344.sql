SELECT id, method, paid_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) AS row_num
    FROM payments
) AS r
WHERE row_num <= 5
ORDER BY 2
