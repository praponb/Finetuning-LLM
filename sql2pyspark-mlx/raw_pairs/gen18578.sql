SELECT carrier, COUNT(*) AS row_count FROM shipments WHERE MONTH(delivered_date) IN (7, 10, 11) GROUP BY carrier ORDER BY carrier DESC;
