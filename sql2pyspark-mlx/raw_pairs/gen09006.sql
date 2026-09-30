WITH ranked_shipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date DESC, id) AS pos FROM shipments) SELECT * FROM ranked_shipments WHERE pos <= 2 ORDER BY carrier
