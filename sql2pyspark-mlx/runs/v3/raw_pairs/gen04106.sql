SELECT
    s.id,
    s.store_name,
    COUNT(o.id) AS order_count,
    MIN(o.order_date) AS first_order_date
FROM stores s
LEFT JOIN orders o ON s.id = o.store_id
GROUP BY s.id, s.store_name
