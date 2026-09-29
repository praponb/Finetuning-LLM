SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.amount,
    s.shipping_fee,
    b.sqft
FROM orders s
LEFT JOIN stores b
    ON s.store_id = b.id;
