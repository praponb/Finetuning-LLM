SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST, id ASC) AS latest_rank FROM shipments) x WHERE latest_rank = 1;
