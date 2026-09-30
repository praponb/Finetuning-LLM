SELECT
    o.shipping_fee,
    c.signup_date,
    c.city,
    s.opened_date,
    TRIM(s.store_name) AS clean_store_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
LEFT JOIN stores s ON o.store_id = s.id
