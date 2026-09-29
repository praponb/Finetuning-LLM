select case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end as quantity_bucket, max(quantity) - min(quantity) as quantity_range
from order_items
where quantity < 3
group by case when quantity >= 8 then 'high' when quantity >= 3 then 'mid' else 'low' end
order by 1;
