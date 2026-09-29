SELECT COUNT(DISTINCT cost) AS unique_carriers, MIN(delivered_date) AS min_delivered_date, MAX(cost) - MIN(cost) AS cost_range
FROM shipments
WHERE shipped_date < '2023-12-01';
