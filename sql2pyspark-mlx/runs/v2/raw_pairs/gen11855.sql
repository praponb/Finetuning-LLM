SELECT id, order_id, cost, RANK() OVER (PARTITION BY order_id ORDER BY cost DESC) AS rnk FROM shipments
