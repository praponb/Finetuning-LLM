WITH ranked_orders AS (
    SELECT
        id,
        status,
        discount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount DESC, id) AS rn
    FROM orders
    WHERE status IN ('SHIPPED', 'COMPLETED', 'RETURNED')
)
SELECT *
FROM ranked_orders
WHERE rn <= 2
