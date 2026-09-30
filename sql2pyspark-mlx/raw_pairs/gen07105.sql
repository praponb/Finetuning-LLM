SELECT status, COUNT(cost) AS cost_count, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count
FROM shipments
WHERE MONTH(delivered_date) IN (6, 8, 9)
GROUP BY status
ORDER BY cost_count DESC;
