select order_id, sum(case when status = 'LOST' then cost else 0 end) as lost_cost
from shipments
group by order_id
order by order_id, lost_cost desc;
