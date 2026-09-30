select case when o.status in ('CANCELLED', 'RETURNED') then 'group_a' else 'other' end as status_group, max(oi.quantity) as max_quantity, count(oi.quantity) as quantity_count
from order_items oi
inner join orders o on oi.order_id = o.id
left join products p on oi.product_id = p.id
group by case when o.status in ('CANCELLED', 'RETURNED') then 'group_a' else 'other' end
having max_quantity >= 5.7
order by max_quantity, status_group
limit 1;
