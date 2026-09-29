WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) AS pos
    FROM orders
)
SELECT
    id,
    channel,
    discount,
    customer_id
FROM ranked
WHERE pos <= 3
ORDER BY id ASC, channel DESC;
