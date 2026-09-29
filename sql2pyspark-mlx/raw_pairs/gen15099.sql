WITH shipments_by_order AS (
    SELECT order_id, SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END) kerry_count, MIN(cost) min_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.status,
    sbo.kerry_count,
    sbo.min_cost
FROM orders o
INNER JOIN shipments_by_order sbo ON o.id = sbo.order_id
ORDER BY kerry_count DESC, o.shipping_fee DESC;
