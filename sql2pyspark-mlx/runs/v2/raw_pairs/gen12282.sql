WITH top_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS rn
    FROM shipments
)
SELECT
    id,
    carrier,
    shipped_date,
    order_id
FROM top_shipments
WHERE rn <= 5
ORDER BY order_id ASC;
