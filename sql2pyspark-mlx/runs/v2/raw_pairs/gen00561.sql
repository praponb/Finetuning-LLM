WITH Ranked AS (
    SELECT
        id,
        status,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS row_num
    FROM shipments
)
SELECT id, status, shipped_date
FROM Ranked
WHERE row_num = 1
ORDER BY id DESC;
