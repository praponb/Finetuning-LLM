SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, COUNT(cost) AS cost_count
FROM shipments
WHERE status IN ('RETURNED', 'IN_TRANSIT')
GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM')
HAVING COUNT(cost) >= 4;
