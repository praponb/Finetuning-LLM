SELECT QUARTER(delivered_date) AS delivered_quarter, MAX(cost) AS max_cost, COUNT(*) AS shipment_count FROM shipments GROUP BY QUARTER(delivered_date) HAVING MAX(cost) < 82 ORDER BY max_cost DESC
