WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num
    FROM payments
)
SELECT *
FROM numbered
WHERE row_num <= 1
ORDER BY status;
