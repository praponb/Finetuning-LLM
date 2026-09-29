WITH ranked_shipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS row_num FROM shipments) SELECT * FROM ranked_shipments WHERE row_num <= 1
