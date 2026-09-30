SELECT
    oi.product_id,
    o.shipping_fee,
    o.order_date,
    p.brand,
    p.product_name
FROM order_items oi
RIGHT OUTER JOIN orders o ON oi.order_id = o.id
INNER JOIN products p ON oi.product_id = p.id
WHERE o.channel IN ('partner', 'mobile', 'store') OR p.price BETWEEN 720 AND 1140
