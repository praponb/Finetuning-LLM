select case when ss.units >= 36 then 'high' when ss.units >= 20 then 'mid' else 'low' end as units_bucket, sum(i.quantity) as total_quantity, round(avg(i.quantity), 2) as avg_quantity
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by case when ss.units >= 36 then 'high' when ss.units >= 20 then 'mid' else 'low' end;
