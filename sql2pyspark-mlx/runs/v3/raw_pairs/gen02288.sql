SELECT
    o.order_date,
    s.opened_date,
    s.sqft,
    s.store_name
FROM orders o
JOIN stores s ON o.store_id = s.id;
