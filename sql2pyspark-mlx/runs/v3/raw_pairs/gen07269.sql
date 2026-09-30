WITH ranked_shipments AS (
    SELECT
        id,
        order_id,
        cost,
        shipped_date,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost DESC) AS rnk
    FROM shipments
    WHERE status = 'DELIVERED'
)
SELECT *
FROM ranked_shipments
WHERE rnk = 1
ORDER BY id;
