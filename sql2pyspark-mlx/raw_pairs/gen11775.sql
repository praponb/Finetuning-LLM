SELECT status, MAX(shipped_date) AS last_shipped_date FROM shipments GROUP BY 1 ORDER BY last_shipped_date DESC;
