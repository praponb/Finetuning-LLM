SELECT
    s.city,
    s.sqft,
    s.opened_date,
    o.order_date
FROM stores s
LEFT OUTER JOIN orders o ON s.id = o.store_id
