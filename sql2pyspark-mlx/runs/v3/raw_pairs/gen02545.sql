select carrier, sum(cost) as total_cost, sum(sum(cost)) over (order by carrier) as cumulative_cost
from shipments
where status in ('LOST', 'RETURNED')
group by carrier
order by carrier asc, total_cost desc;
