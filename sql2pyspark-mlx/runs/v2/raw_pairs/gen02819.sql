SELECT
    oi.order_id,
    oi.product_id,
    oi.unit_price,
    o.discount
FROM order_items oi
RIGHT JOIN orders o ON oi.order_id = o.id
WHERE o.order_date <= '2023-06-01'
