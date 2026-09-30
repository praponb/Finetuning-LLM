SELECT
    s.status,
    s.order_id,
    o.channel,
    st.store_name
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON st.id = o.store_id
WHERE o.order_date > '2024-12-21'
ORDER BY order_id DESC
