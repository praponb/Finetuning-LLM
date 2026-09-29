WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS rnk
    FROM shipments
)
SELECT
    id,
    carrier,
    delivered_date,
    shipped_date
FROM top_shipments
WHERE rnk = 1
ORDER BY delivered_date DESC
