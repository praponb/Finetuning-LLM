SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, ROUND(AVG(cost), 2) AS avg_cost, ROUND((100 * SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_kerry
FROM shipments
GROUP BY DATE_FORMAT(shipped_date, 'yyyy-MM')
ORDER BY 3 DESC;
