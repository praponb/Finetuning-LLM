select id, order_id, cost, max(cost) over (partition by order_id) as order_id_max_cost from shipments where status <> 'LOST' order by cost
