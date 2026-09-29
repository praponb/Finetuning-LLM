select case when o.amount >= 1790 then 'high' when o.amount >= 460 then 'mid' else 'low' end as amount_bucket, max(oi.quantity) as max_quantity
from order_items oi
inner join products p on p.id = oi.product_id
left join orders o on oi.order_id = o.id
where o.store_id is not null
group by case when o.amount >= 1790 then 'high' when o.amount >= 460 then 'mid' else 'low' end
