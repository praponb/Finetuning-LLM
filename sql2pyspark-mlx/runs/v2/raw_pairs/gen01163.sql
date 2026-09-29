SELECT
    s.carrier,
    o.status,
    o.channel,
    st.opened_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON o.store_id = st.id
WHERE s.cost <= 57 AND s.cost >= 76
ORDER BY o.channel DESC;
