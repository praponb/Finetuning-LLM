SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') shipped_month, AVG(cost) avg_cost
FROM shipments
WHERE carrier IN ('Kerry', 'FedEx', 'UPS')
GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM')
ORDER BY avg_cost DESC;
