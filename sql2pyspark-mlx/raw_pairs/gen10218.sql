SELECT
    orders.discount,
    orders.channel,
    t.shipment_count,
    t.cnt
FROM orders
INNER JOIN (SELECT order_id, COUNT(*) AS shipment_count, COUNT(*) AS cnt
    FROM shipments
    WHERE cost >= 46
    GROUP BY order_id) t ON orders.id = t.order_id;
