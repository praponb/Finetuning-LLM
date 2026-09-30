SELECT status, COUNT(*) AS cnt, COUNT(*) AS shipment_count, ROUND(AVG(cost), 2) AS avg_cost FROM shipments WHERE cost > 40 AND (cost BETWEEN 29 AND 51 OR order_id = 4) GROUP BY status;
