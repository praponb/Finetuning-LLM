SELECT
    st.city,
    DATE_TRUNC('MONTH', st.opened_date) AS opened_month,
    ROUND(AVG(s.cost), 2) AS avg_cost,
    COUNT(s.cost) AS cost_count
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
JOIN stores st ON o.store_id = st.id
GROUP BY st.city, DATE_TRUNC('MONTH', st.opened_date)
ORDER BY city DESC;
