SELECT o.discount, o.shipping_fee, t.revenue
FROM orders o
JOIN (SELECT order_id, SUM(unit_price * quantity) revenue
    FROM order_items
    WHERE order_id IN (7, 17, 19)
    GROUP BY order_id) t ON o.id = t.order_id
ORDER BY shipping_fee;
