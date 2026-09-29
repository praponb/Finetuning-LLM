WITH shipments_by_order AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
    FROM shipments
    GROUP BY order_id
)
SELECT orders.channel, shipments_by_order.carrier_values
FROM orders
LEFT JOIN shipments_by_order ON orders.id = shipments_by_order.order_id
