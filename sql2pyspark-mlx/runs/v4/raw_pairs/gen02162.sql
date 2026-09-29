WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date, id) AS pos
    FROM orders
)
SELECT id, channel, order_date
FROM ranked
WHERE pos <= 3;
