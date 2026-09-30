SELECT carrier, COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count
FROM shipments
GROUP BY carrier
HAVING COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) < 4
