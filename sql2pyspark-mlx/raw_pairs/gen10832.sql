SELECT CASE WHEN cost >= 76 THEN 'high' WHEN cost >= 19 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count
FROM shipments
WHERE status = 'LOST'
GROUP BY cost_bucket
HAVING COUNT(*) >= 5
