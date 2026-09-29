SELECT
    s.id,
    s.status,
    s.order_id,
    o.order_date,
    o.channel,
    st.city,
    st.store_name
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
JOIN stores st ON st.id = o.store_id;
