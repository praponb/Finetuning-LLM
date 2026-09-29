WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY order_date) AS pos
    FROM orders
)
SELECT
    id,
    store_id,
    order_date,
    discount
FROM ranked
WHERE pos = 1
ORDER BY discount DESC, order_date
