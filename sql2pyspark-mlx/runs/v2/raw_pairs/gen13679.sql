SELECT
    s.id,
    s.order_id,
    o.order_date,
    st.store_name
FROM shipments s
LEFT OUTER JOIN orders o ON o.id = s.order_id
JOIN stores st ON o.store_id = st.id
WHERE o.store_id IS NULL AND o.shipping_fee <= 13;
