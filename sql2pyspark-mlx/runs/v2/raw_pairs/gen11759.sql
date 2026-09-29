select *
from (
    select case when unit_price >= 450 then 'high' when unit_price >= 60 then 'mid' else 'low' end as unit_price_bucket, case when quantity >= 8 then 'high' when quantity >= 2 then 'mid' else 'low' end as quantity_bucket, count(*) as total_count
    from order_items
    where quantity = 8
    group by case when unit_price >= 450 then 'high' when unit_price >= 60 then 'mid' else 'low' end, case when quantity >= 8 then 'high' when quantity >= 2 then 'mid' else 'low' end
) as x
where total_count >= 9
order by quantity_bucket
