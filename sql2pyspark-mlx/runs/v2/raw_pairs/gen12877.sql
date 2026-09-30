WITH ranked_shipments AS (SELECT *, RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS pos FROM shipments) SELECT * FROM ranked_shipments WHERE pos <= 3
