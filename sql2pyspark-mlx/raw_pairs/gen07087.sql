SELECT
    o.id,
    o.amount,
    s.sqft,
    c.city,
    c.signup_date,
    CONCAT_WS(',', s.city, s.region) AS city_region_key
FROM orders o
RIGHT JOIN stores s ON s.id = o.store_id
LEFT JOIN customers c ON c.id = o.customer_id
WHERE o.shipping_fee > 17
ORDER BY c.city DESC
