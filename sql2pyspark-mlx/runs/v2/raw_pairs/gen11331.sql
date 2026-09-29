SELECT MONTH(delivered_date) AS delivered_month, COUNT(*) AS num_shipments, MAX(shipped_date) AS latest_shipped_date
FROM shipments
GROUP BY 1
HAVING COUNT(*) >= 4;
