WITH ranked_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS pos
    FROM shipments
)
SELECT id, carrier, shipped_date
FROM ranked_shipments
WHERE pos <= 3
ORDER BY id DESC
