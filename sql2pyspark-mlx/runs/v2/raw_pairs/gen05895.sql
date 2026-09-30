SELECT carrier, status, ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_fedex, COUNT(cost) cost_count FROM shipments GROUP BY carrier, status
