SELECT id, order_id, cost, DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost) AS dense_rnk, COUNT(*) OVER (PARTITION BY order_id) AS order_id_count FROM shipments WHERE carrier = 'FedEx'
