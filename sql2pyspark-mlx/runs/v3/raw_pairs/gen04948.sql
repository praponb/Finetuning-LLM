SELECT
    s.id,
    s.cost,
    o.discount,
    o.store_id,
    st.city
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN stores st ON st.id = o.store_id;
