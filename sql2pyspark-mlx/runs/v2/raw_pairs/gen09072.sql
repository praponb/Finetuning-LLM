SELECT
    s.carrier,
    o.shipping_fee,
    st.region,
    st.city
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
INNER JOIN stores st ON o.store_id = st.id
