WITH shipments_by_order AS (
    SELECT order_id, COUNT(CASE WHEN carrier = 'DHL' THEN 1 END) AS dhl_count
    FROM shipments
    WHERE carrier IN ('FedEx', 'DHL', 'Flash')
    GROUP BY order_id
)
SELECT orders.id, orders.order_date, shipments_by_order.dhl_count
FROM orders
LEFT JOIN shipments_by_order ON orders.id = shipments_by_order.order_id
ORDER BY shipments_by_order.dhl_count DESC;
