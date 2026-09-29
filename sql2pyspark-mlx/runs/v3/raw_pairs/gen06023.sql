select case when cost >= 92 then 'high' when cost >= 44 then 'mid' else 'low' end as cost_bucket, count(cost) as cost_count
from shipments
group by 1
having sum(cost) > 290
order by cost_count desc;
