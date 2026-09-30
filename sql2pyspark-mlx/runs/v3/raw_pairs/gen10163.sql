SELECT
    s.shipped_date,
    o.status,
    o.shipping_fee,
    st.region,
    st.city
FROM shipments s
RIGHT JOIN orders o ON o.id = s.order_id
FULL OUTER JOIN stores st ON o.store_id = st.id
WHERE st.city IN ('New York', 'Tokyo');
