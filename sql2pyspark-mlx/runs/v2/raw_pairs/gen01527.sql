SELECT
    s.id,
    s.status,
    o.store_id,
    o.order_date,
    st.store_name
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
RIGHT JOIN stores st ON st.id = o.store_id
WHERE st.store_name LIKE '%Old%' AND s.carrier NOT IN ('DHL', 'UPS')
ORDER BY o.order_date ASC
