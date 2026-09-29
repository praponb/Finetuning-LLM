SELECT
    st.city,
    st.region,
    SUM(s.cost) AS total_cost,
    ROUND(AVG(s.cost), 2) AS avg_cost
FROM shipments s
FULL OUTER JOIN orders o ON s.order_id = o.id
LEFT JOIN stores st ON st.id = o.store_id
GROUP BY st.city, st.region
ORDER BY total_cost DESC, city, region
LIMIT 10
