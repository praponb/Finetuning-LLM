SELECT
    s.id,
    s.order_id,
    o.channel,
    o.customer_id,
    st.region,
    st.store_name
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE st.sqft >= 8989 AND o.shipping_fee BETWEEN 26 AND 39;
