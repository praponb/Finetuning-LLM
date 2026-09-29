SELECT cost_bucket, pct_fedex, in_transit_count
FROM (
    SELECT CASE WHEN cost >= 77 THEN 'high' WHEN cost >= 28 THEN 'mid' ELSE 'low' END AS cost_bucket, ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex, COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END) AS in_transit_count
    FROM shipments
    GROUP BY CASE WHEN cost >= 77 THEN 'high' WHEN cost >= 28 THEN 'mid' ELSE 'low' END
) AS t
WHERE pct_fedex >= 28
ORDER BY pct_fedex DESC, cost_bucket;
