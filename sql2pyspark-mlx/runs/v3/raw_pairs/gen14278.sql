select
    category,
    case when cost >= 750 then 'high' when cost >= 350 then 'mid' else 'low' end cost_bucket,
    avg(cost) avg_cost,
    sum(cost) total_cost
from products
where supplier_id is not null and supplier_id is not null
group by cube(category, case when cost >= 750 then 'high' when cost >= 350 then 'mid' else 'low' end)
order by total_cost;
