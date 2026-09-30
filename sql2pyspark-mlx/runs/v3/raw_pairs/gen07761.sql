SELECT
    oi.id,
    oi.unit_price,
    o.store_id,
    s.city,
    s.store_name
FROM order_items oi
INNER JOIN orders o ON oi.order_id = o.id
RIGHT OUTER JOIN stores s ON o.store_id = s.id
WHERE s.region <> 'North'
ORDER BY oi.id DESC, 4;
