WITH top_shipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS rn FROM shipments) SELECT id, carrier, cost, shipped_date FROM top_shipments WHERE rn = 1;
