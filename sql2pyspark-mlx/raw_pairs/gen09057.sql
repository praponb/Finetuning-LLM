SELECT DATE_FORMAT(delivered_date, 'yyyy-MM') delivered_month, MIN(delivered_date) first_delivered_date, COUNT(DISTINCT cost) unique_carriers
FROM shipments
WHERE carrier = 'Kerry'
GROUP BY DATE_FORMAT(delivered_date, 'yyyy-MM')
ORDER BY delivered_month;
