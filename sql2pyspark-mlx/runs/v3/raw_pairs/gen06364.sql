select case when cost >= 91 then 'high' when cost >= 31 then 'mid' else 'low' end as cost_bucket, min(cost) as min_cost, count(distinct cost) as unique_statuss
from shipments
group by 1
having unique_statuss > 3
order by min_cost desc, cost_bucket
limit 50;
