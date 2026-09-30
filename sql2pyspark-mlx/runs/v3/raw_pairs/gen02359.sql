SELECT carrier, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
WHERE status = 'RETURNED'
GROUP BY carrier
ORDER BY carrier
