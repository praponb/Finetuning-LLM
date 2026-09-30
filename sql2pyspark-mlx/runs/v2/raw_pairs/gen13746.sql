select
    case when cost >= 101 then 'high' when cost >= 31 then 'mid' else 'low' end as cost_bucket,
    sum(cost) as total_cost,
    count(*) as cnt,
    sum(sum(cost)) over (order by case when cost >= 101 then 'high' when cost >= 31 then 'mid' else 'low' end) as cumulative_cost
from shipments
group by case when cost >= 101 then 'high' when cost >= 31 then 'mid' else 'low' end
order by cost_bucket
