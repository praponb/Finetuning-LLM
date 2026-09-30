WITH top_shipments AS (
    SELECT
        id,
        status,
        cost,
        delivered_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost DESC, id) AS pos
    FROM shipments
)
SELECT *
FROM top_shipments
WHERE pos = 1
