SELECT
    st.region,
    st.city,
    ROUND((100 * SUM(CASE WHEN s.carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex,
    ROUND(AVG(s.cost), 2) AS avg_cost
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id
GROUP BY st.region, st.city
