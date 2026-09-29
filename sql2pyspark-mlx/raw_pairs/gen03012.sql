select
    case when price >= 920 then 'high' when price >= 450 then 'mid' else 'low' end as price_bucket,
    sum(cost) as total_cost,
    count(*) as cnt,
    sum(sum(cost)) over (order by case when price >= 920 then 'high' when price >= 450 then 'mid' else 'low' end) as cumulative_cost
from products
where brand in ('Acme', 'Initech')
group by case when price >= 920 then 'high' when price >= 450 then 'mid' else 'low' end;
