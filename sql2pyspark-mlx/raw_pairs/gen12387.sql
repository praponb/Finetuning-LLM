WITH top_shipments AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost, id) AS rnk FROM shipments) SELECT id, status, cost, order_id FROM top_shipments WHERE rnk <= 5
