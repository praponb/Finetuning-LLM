SELECT
    oi.id,
    oi.quantity,
    oi.product_id,
    oi.unit_price,
    o.store_id,
    o.amount,
    o.status
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
WHERE o.order_date BETWEEN '2023-05-01' AND '2024-05-01';
