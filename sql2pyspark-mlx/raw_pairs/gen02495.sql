WITH ranked_shipments AS (
    SELECT
        id,
        status,
        shipped_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS rn
    FROM shipments
)
SELECT id, status, shipped_date
FROM ranked_shipments
WHERE rn <= 2;
