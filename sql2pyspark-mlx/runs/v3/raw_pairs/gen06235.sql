SELECT SUM(cost) AS total_cost, COUNT(*) AS num_shipments, COUNT(DISTINCT cost) AS unique_carriers
FROM shipments
