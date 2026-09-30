SELECT
    oi.order_id,
    p.price,
    o.customer_id,
    o.amount - o.shipping_fee AS amount_with_shipping
FROM order_items oi
LEFT OUTER JOIN products p ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
ORDER BY price, oi.order_id DESC;
