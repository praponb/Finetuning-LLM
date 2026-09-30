SELECT
    s.id,
    s.status,
    o.status AS order_status,
    o.order_date,
    st.opened_date,
    st.city
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
RIGHT JOIN stores st ON o.store_id = st.id
WHERE s.cost < 90
ORDER BY st.opened_date;
