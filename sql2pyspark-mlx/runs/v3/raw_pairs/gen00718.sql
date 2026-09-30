SELECT cost_bucket, avg_cost, in_transit_cost
FROM (
    SELECT CASE WHEN cost >= 87 THEN 'high' WHEN cost >= 37 THEN 'mid' ELSE 'low' END AS cost_bucket, AVG(cost) AS avg_cost, SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost ELSE 0 END) AS in_transit_cost
    FROM shipments
    GROUP BY CASE WHEN cost >= 87 THEN 'high' WHEN cost >= 37 THEN 'mid' ELSE 'low' END
) AS sub
WHERE in_transit_cost > 70;
