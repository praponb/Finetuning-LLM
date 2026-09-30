WITH top_shipments AS (SELECT id, carrier, cost, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) rnk FROM shipments) SELECT id, carrier, cost FROM top_shipments WHERE rnk <= 3
