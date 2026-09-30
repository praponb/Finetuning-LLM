SELECT
    o.status,
    o.shipping_fee,
    c.credit_limit,
    c.signup_date,
    s.sqft
FROM orders o
LEFT OUTER JOIN customers c ON o.customer_id = c.id
LEFT OUTER JOIN stores s ON o.store_id = s.id
WHERE c.city = 'Chiang Mai'
