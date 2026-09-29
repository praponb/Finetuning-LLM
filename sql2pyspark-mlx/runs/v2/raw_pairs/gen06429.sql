SELECT
    s.order_id,
    s.carrier,
    o.discount,
    o.amount,
    st.region,
    st.opened_date
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
JOIN stores st ON st.id = o.store_id
WHERE o.channel = 'web' AND s.delivered_date <= '2025-07-26'
ORDER BY order_id DESC
