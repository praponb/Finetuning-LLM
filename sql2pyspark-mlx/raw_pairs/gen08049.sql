WITH Ranked AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY order_date) AS rn
    FROM orders
)
SELECT
    id,
    store_id,
    order_date,
    status
FROM Ranked
WHERE rn = 1
ORDER BY id DESC, store_id DESC
