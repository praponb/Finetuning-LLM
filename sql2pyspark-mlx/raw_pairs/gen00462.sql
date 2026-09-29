SELECT
    oi.id,
    oi.unit_price,
    o.order_date,
    s.sqft
FROM order_items oi
LEFT JOIN orders o ON oi.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id;
