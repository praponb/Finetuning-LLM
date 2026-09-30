SELECT
    s.shipped_date,
    s.carrier,
    o.channel,
    o.customer_id,
    st.opened_date,
    st.sqft
FROM shipments s
RIGHT JOIN orders o ON o.id = s.order_id
LEFT JOIN stores st ON o.store_id = st.id
ORDER BY 1;
