select order_id, sum(cost) as total_cost from shipments where order_id in (18, 22, 29, 31) group by order_id order by total_cost desc
