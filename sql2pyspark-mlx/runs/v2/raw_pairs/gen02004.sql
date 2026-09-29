SELECT status, carrier, MAX(cost) AS max_cost FROM shipments GROUP BY status, carrier HAVING SUM(CASE WHEN status = 'DELIVERED' THEN cost ELSE 0 END) > 49;
