SELECT
    s.cost,
    o.customer_id,
    o.amount,
    st.opened_date
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN stores st ON o.store_id = st.id;
