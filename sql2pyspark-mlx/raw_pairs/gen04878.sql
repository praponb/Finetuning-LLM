SELECT DATE_FORMAT(delivered_date, 'yyyy-MM') AS delivered_month, ROUND((100 * SUM(CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_kerry FROM shipments GROUP BY delivered_month
