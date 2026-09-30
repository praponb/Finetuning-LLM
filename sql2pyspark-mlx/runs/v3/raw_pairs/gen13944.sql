SELECT CASE WHEN status IN ('LOST', 'DELIVERED') THEN 'core' ELSE 'other' END AS status_group, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM shipments
GROUP BY CASE WHEN status IN ('LOST', 'DELIVERED') THEN 'core' ELSE 'other' END
