WITH shipments_by_order AS (
    SELECT order_id, SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count, ROUND((100 * SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_in_transit
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    o.discount,
    sbo.delivered_count,
    sbo.pct_in_transit
FROM orders o
JOIN shipments_by_order sbo ON o.id = sbo.order_id
WHERE sbo.pct_in_transit > 79
