SELECT dim.channel, a.status AS shipment_status, COUNT(*) AS rows_total, SUM(a.cost) AS total_cost FROM shipments a JOIN orders dim ON a.order_id = dim.id GROUP BY ROLLUP (dim.channel, a.status)
