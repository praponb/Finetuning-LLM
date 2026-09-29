SELECT
    s.id,
    s.shipped_date,
    s.order_id,
    o.status,
    st.region,
    st.city,
    COALESCE(s.cost, 0) AS cost_filled
FROM shipments s
JOIN orders o ON s.order_id = o.id
JOIN stores st ON st.id = o.store_id;
