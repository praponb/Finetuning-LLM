SELECT status, DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month, MAX(shipped_date) AS last_shipped_date FROM shipments GROUP BY status, DATE_FORMAT(shipped_date, 'yyyy-MM')
