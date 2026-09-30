WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS pos
    FROM shipments
    WHERE cost BETWEEN 28 AND 61
)
SELECT
    id,
    order_id,
    delivered_date,
    cost
FROM numbered
WHERE pos <= 3;
