select case when price >= 1250 then 'high' when price >= 240 then 'mid' else 'low' end as price_bucket, max(cost) - min(cost) as cost_range, max(cost) as max_cost
from products
group by case when price >= 1250 then 'high' when price >= 240 then 'mid' else 'low' end
