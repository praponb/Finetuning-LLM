select status, sum(case when carrier = 'Flash' then cost else 0 end) as flash_cost, max(cost) - min(cost) as cost_range
from shipments
group by status
order by cost_range desc
