select case when ss.units >= 37 then 'high' when ss.units >= 8 then 'mid' else 'low' end as units_bucket, avg(i.quantity) as avg_quantity, count(i.quantity) as quantity_count
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where i.store_id = 1
group by case when ss.units >= 37 then 'high' when ss.units >= 8 then 'mid' else 'low' end
order by units_bucket
limit 50;
