SELECT
    o.id,
    o.discount,
    o.order_date,
    c.signup_date,
    s.opened_date,
    s.store_name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
INNER JOIN stores s ON o.store_id = s.id
