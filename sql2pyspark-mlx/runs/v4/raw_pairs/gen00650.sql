SELECT
    s.id,
    s.status,
    s.carrier,
    o.shipping_fee,
    o.status AS order_status,
    st.opened_date,
    st.sqft,
    TRIM(o.channel) AS clean_channel
FROM shipments s
LEFT JOIN orders o ON o.id = s.order_id
LEFT JOIN stores st ON o.store_id = st.id
ORDER BY o.shipping_fee
