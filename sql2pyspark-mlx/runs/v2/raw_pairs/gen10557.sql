SELECT
    s.order_id,
    o.order_date,
    o.amount,
    st.sqft,
    st.opened_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON o.store_id = st.id
WHERE o.channel IS NOT NULL;
