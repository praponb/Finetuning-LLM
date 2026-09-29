SELECT
    o.id,
    o.shipping_fee,
    o.customer_id,
    o.order_date,
    c.city
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE c.segment = 'Consumer' AND o.amount <= 790
ORDER BY 1
