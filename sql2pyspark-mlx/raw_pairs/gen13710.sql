SELECT
    s.id,
    s.status,
    o.discount,
    o.channel,
    st.sqft,
    st.opened_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id
ORDER BY status, id
LIMIT 3;
