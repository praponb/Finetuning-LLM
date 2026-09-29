SELECT
    a.id,
    a.order_id,
    a.product_id,
    d.shipping_fee,
    d.status,
    d.discount
FROM order_items a
LEFT JOIN orders d
    ON a.order_id = d.id
