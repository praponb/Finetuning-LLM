SELECT
    c.id,
    c.name,
    COUNT(o.id) AS order_count,
    SUM(o.shipping_fee) AS total_shipping_fee
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE c.city = 'Chiang Mai'
GROUP BY c.id, c.name
HAVING SUM(o.shipping_fee) >= 160;
