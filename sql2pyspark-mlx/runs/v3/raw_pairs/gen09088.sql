select case when o.amount >= 1690 then 'high' when o.amount >= 760 then 'mid' else 'low' end as amount_bucket, count(distinct oi.quantity) as unique_orders, count(*) as cnt
from order_items oi
inner join orders o on oi.order_id = o.id
inner join products p on p.id = oi.product_id
group by case when o.amount >= 1690 then 'high' when o.amount >= 760 then 'mid' else 'low' end
order by amount_bucket
