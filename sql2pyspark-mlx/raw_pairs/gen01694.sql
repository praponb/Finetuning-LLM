SELECT
    s.status,
    s.shipped_date,
    o.customer_id,
    st.city,
    TRIM(o.status) AS clean_status
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN stores st ON o.store_id = st.id
WHERE s.shipped_date BETWEEN '2025-03-01' AND '2025-04-01';
