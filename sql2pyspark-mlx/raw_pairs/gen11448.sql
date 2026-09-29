select id, cost, rank() over (order by cost) as cost_rank from shipments
