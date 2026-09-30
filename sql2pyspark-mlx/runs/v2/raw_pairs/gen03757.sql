select case when ss.revenue >= 3700 then 'high' when ss.revenue >= 700 then 'mid' else 'low' end as revenue_bucket, avg(i.quantity) as avg_quantity
from inventory i
left join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
group by case when ss.revenue >= 3700 then 'high' when ss.revenue >= 700 then 'mid' else 'low' end
having avg_quantity > 120
order by revenue_bucket desc;
