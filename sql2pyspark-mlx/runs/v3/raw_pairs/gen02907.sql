SELECT DISTINCT carrier
FROM shipments
WHERE status = 'LOST'
ORDER BY carrier DESC
