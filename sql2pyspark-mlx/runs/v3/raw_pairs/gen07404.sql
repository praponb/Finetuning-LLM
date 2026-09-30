SELECT carrier, COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count
FROM shipments
WHERE status = 'LOST'
GROUP BY carrier
