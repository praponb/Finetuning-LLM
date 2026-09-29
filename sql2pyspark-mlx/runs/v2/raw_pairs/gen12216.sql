SELECT SUM(CASE WHEN status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count, MIN(delivered_date) AS min_delivered_date, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
ORDER BY avg_cost DESC;
