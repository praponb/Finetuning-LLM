SELECT status, carrier, COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count
FROM shipments
GROUP BY status, carrier
ORDER BY status, carrier
LIMIT 3;
