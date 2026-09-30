select case when o.discount >= 0.41 then 'high' when o.discount >= 0.11 then 'mid' else 'low' end as discount_bucket, round(stddev(oi.quantity), 2) as stddev_quantity, count(oi.quantity) as quantity_count
from order_items oi
left join orders o on oi.order_id = o.id
where o.channel in ('web', 'mobile')
group by case when o.discount >= 0.41 then 'high' when o.discount >= 0.11 then 'mid' else 'low' end
order by discount_bucket, quantity_count desc
