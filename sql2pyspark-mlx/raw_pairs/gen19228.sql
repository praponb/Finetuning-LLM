with ranked_shipments as (select *, rank() over (partition by status order by cost desc) pos from shipments) select id, status, cost, carrier from ranked_shipments where pos <= 1
