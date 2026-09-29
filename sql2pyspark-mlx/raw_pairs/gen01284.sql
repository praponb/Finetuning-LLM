WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY shipped_date DESC, id) AS rn
        FROM shipments
    ) x
    WHERE rn = 1
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost
FROM latest
