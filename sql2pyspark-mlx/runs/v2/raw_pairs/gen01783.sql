SELECT
    s.id,
    s.carrier,
    s.cost,
    o.amount,
    st.sqft
FROM shipments s
JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id
