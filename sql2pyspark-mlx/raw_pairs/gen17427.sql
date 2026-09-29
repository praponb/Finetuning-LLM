SELECT *
FROM (
    SELECT
        order_id,
        carrier,
        status,
        cost
    FROM shipments
) src
PIVOT (
    AVG(cost)
    FOR status IN ('LOST' AS lost, 'DELIVERED' AS delivered, 'RETURNED' AS returned)
)
