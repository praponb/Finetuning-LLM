select status, count(*) as num_shipments, min(cost) as min_cost
from shipments
where carrier in ('Kerry', 'Flash')
group by status
having min(cost) < 90
order by status desc
