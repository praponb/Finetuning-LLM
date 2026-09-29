SELECT st.city, MAX(s.cost) max_cost, MAX(s.cost) - MIN(s.cost) cost_range
FROM shipments s
LEFT OUTER JOIN orders o ON s.order_id = o.id
JOIN stores st ON o.store_id = st.id
GROUP BY st.city
ORDER BY city
