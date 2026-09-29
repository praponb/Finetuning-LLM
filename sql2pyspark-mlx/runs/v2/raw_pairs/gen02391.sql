SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, carrier, COUNT(*) AS row_count
FROM shipments
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY carrier, shipped_month
LIMIT 50;
