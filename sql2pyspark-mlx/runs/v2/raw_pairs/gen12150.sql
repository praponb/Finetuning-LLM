WITH ranked_shipments AS (
    SELECT
        id,
        carrier,
        delivered_date,
        order_id,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS rn
    FROM shipments
    WHERE order_id IS NOT NULL
)
SELECT
    id,
    carrier,
    delivered_date,
    order_id
FROM ranked_shipments
WHERE rn = 1
