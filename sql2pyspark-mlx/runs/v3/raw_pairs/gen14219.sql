SELECT id, order_id, cost
FROM (
    SELECT
        id,
        order_id,
        cost,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost) AS rn
    FROM shipments
) AS t
WHERE rn = 1;
