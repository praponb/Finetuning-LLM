SELECT MAX(cost) - MIN(cost) AS cost_range, COUNT(DISTINCT cost) AS unique_carriers, MAX(shipped_date) AS last_shipped_date
FROM shipments;
