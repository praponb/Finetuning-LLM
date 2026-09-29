SELECT
    oi.id,
    oi.order_id,
    o.discount,
    o.status,
    s.city,
    INITCAP(TRIM(s.region)) AS region_title
FROM order_items oi
RIGHT OUTER JOIN orders o ON oi.order_id = o.id
RIGHT OUTER JOIN stores s ON o.store_id = s.id;
