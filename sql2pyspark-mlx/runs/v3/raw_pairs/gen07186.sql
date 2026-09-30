SELECT
    s.shipped_date,
    s.carrier,
    o.channel,
    o.shipping_fee,
    st.opened_date,
    st.city
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
LEFT OUTER JOIN stores st ON st.id = o.store_id
WHERE st.opened_date > '2024-08-08'
ORDER BY st.city;
