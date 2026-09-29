SELECT
    s.city,
    s.store_name,
    s.opened_date,
    o.discount,
    o.order_date,
    o.customer_id
FROM stores s
LEFT JOIN orders o ON o.store_id = s.id;
