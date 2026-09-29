SELECT QUARTER(delivered_date) AS delivered_quarter, COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count
FROM shipments
GROUP BY QUARTER(delivered_date)
ORDER BY ups_count
