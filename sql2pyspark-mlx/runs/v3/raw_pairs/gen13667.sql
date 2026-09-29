WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) row_num
    FROM shipments
    WHERE shipped_date > '2025-05-01'
)
SELECT
    id,
    status,
    cost,
    carrier
FROM top_shipments
WHERE row_num = 1;
