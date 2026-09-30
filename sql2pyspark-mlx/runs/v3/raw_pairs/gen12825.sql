SELECT
    o.shipping_fee,
    o.customer_id,
    t.success_count,
    t.card_count
FROM orders o
LEFT JOIN (SELECT order_id, COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count
    FROM payments
    WHERE order_id IN (17, 20, 21, 24)
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.success_count > 9
ORDER BY o.shipping_fee DESC;
