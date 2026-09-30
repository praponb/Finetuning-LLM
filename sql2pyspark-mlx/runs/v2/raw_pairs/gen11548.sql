SELECT
    o.order_date,
    o.status,
    s.sqft,
    c.credit_limit
FROM orders o
FULL OUTER JOIN stores s ON o.store_id = s.id
LEFT JOIN customers c ON o.customer_id = c.id;
