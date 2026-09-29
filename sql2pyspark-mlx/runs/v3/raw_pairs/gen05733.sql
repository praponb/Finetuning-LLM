SELECT carrier, MONTH(shipped_date) AS shipped_month, COUNT(DISTINCT cost) AS unique_orders
FROM shipments
WHERE cost >= 27 AND carrier NOT IN ('DHL', 'UPS', 'Kerry') AND delivered_date < '2025-05-01'
GROUP BY carrier, MONTH(shipped_date)
ORDER BY carrier DESC, shipped_month
LIMIT 3
