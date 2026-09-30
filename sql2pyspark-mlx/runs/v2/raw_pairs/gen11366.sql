select case when ss.units >= 33 then 'high' when ss.units >= 12 then 'mid' else 'low' end as units_bucket, count(*) as total_count, min(i.quantity) as min_quantity
from inventory i
inner join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue < 3000
group by case when ss.units >= 33 then 'high' when ss.units >= 12 then 'mid' else 'low' end
order by units_bucket desc, min_quantity;
