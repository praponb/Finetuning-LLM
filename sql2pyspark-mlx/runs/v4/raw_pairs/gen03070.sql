SELECT status, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM shipments
WHERE carrier = 'FedEx'
GROUP BY status
ORDER BY total_cost DESC, status
