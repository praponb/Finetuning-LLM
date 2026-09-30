SELECT st.region, AVG(s.cost) AS avg_cost, COUNT(CASE WHEN s.status = 'RETURNED' THEN 1 END) AS returned_count
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
INNER JOIN stores st ON st.id = o.store_id
GROUP BY st.region
ORDER BY returned_count DESC
