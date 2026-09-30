SELECT
    o.store_id,
    o.order_date,
    o.amount,
    oi.quantity,
    oi.product_id,
    oi.unit_price
FROM orders o
LEFT OUTER JOIN order_items oi ON oi.order_id = o.id
WHERE oi.order_id IN (5, 18)
ORDER BY amount;
