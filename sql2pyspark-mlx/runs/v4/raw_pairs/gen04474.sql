select id, carrier, cost, rank() over (partition by carrier order by cost desc) as cost_rank from shipments order by carrier asc
