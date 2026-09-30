SELECT
    s.shipped_date,
    s.carrier,
    o.status,
    st.city
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON st.id = o.store_id
WHERE st.sqft > 4807 AND st.region IN ('South', 'West', 'East')
