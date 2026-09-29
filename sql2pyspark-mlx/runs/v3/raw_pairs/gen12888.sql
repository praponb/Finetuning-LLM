select id, order_id, carrier, cost, round(cost / sum(cost) over (partition by order_id, carrier), 3) as cost_share from shipments where status not in ('DELIVERED', 'LOST')
