select order_id, cost, carrier, round(cost * 0.85, 2) as cost_scaled from shipments where order_id is not null and order_id in (26, 32) order by order_id
