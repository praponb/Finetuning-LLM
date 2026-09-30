SELECT DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, COUNT(cost) AS cost_count FROM shipments GROUP BY 1 HAVING COUNT(cost) > 1;
