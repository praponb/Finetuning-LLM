WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipped_date DESC) AS pos
    FROM shipments
)
SELECT
    id,
    order_id,
    shipped_date,
    status
FROM numbered
WHERE pos <= 3
ORDER BY id
