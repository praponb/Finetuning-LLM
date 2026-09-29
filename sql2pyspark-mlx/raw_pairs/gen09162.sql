SELECT
    o.id,
    o.order_date,
    o.shipping_fee,
    s.store_name,
    s.city,
    c.age,
    c.email,
    o.amount * o.discount AS amount_times_discount
FROM orders o
JOIN stores s ON o.store_id = s.id
RIGHT JOIN customers c ON o.customer_id = c.id
WHERE c.email LIKE '%@company.co'
