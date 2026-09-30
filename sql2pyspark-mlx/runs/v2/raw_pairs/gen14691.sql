SELECT CASE WHEN o.discount >= 0.32 THEN 'high' WHEN o.discount >= 0.14 THEN 'mid' ELSE 'low' END AS discount_bucket, COUNT(s.cost) AS cost_count, COUNT(DISTINCT s.cost) AS unique_carriers
FROM shipments s
FULL OUTER JOIN orders o ON o.id = s.order_id
WHERE o.discount BETWEEN 0.25 AND 0.38
GROUP BY CASE WHEN o.discount >= 0.32 THEN 'high' WHEN o.discount >= 0.14 THEN 'mid' ELSE 'low' END
HAVING COUNT(DISTINCT s.cost) >= 2
