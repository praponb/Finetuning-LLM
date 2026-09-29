SELECT MAX(cost) - MIN(cost) AS cost_range, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count FROM shipments;
