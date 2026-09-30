SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS row_num
    FROM payments
) AS r
WHERE row_num <= 5
ORDER BY status DESC, method DESC;
