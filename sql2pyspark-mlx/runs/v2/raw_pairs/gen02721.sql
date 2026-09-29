SELECT
    oi.product_id,
    o.shipping_fee,
    p.category,
    o.discount - o.amount discount_minus_amount
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id;
