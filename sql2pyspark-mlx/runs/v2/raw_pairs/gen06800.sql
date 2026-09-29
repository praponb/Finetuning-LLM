SELECT
    s.status,
    o.store_id,
    st.sqft,
    st.opened_date,
    DATE_TRUNC('MONTH', st.opened_date) opened_date_period
FROM shipments s
JOIN orders o ON o.id = s.order_id
JOIN stores st ON o.store_id = st.id
ORDER BY 1 DESC, opened_date;
