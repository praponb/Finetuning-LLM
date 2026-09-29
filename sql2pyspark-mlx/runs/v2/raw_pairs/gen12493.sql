SELECT
    oi.quantity,
    oi.unit_price,
    o.customer_id,
    s.opened_date
FROM order_items oi
LEFT JOIN orders o ON o.id = oi.order_id
INNER JOIN stores s ON o.store_id = s.id
WHERE s.sqft < 5591 OR o.shipping_fee > 36
