SELECT cost, CASE WHEN cost >= 87 THEN 'High' WHEN cost >= 18 THEN 'Medium' ELSE 'Low' END AS cost_tier
FROM shipments
WHERE delivered_date >= '2023-03-01' AND YEAR(delivered_date) >= 2024;
