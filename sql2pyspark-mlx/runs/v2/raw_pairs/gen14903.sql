SELECT YEAR(delivered_date) AS delivered_year, status, MIN(delivered_date) AS min_delivered_date
FROM shipments
WHERE cost BETWEEN 27 AND 85 OR carrier = 'FedEx'
GROUP BY YEAR(delivered_date), status
ORDER BY delivered_year DESC;
