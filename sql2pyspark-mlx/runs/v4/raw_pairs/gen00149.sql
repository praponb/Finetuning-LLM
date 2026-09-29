select case when ss.units >= 45 then 'high' when ss.units >= 9 then 'mid' else 'low' end as units_bucket, avg(i.quantity) as avg_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by case when ss.units >= 45 then 'high' when ss.units >= 9 then 'mid' else 'low' end
having avg_quantity > 90
order by units_bucket desc, avg_quantity;
