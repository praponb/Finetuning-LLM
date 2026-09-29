SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY cost) AS rank_in_group FROM shipments QUALIFY rank_in_group = 1
