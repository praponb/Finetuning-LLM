SELECT
    o.id,
    o.shipping_fee,
    o.channel,
    s.sqft,
    c.signup_date
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
LEFT OUTER JOIN customers c ON c.id = o.customer_id;
