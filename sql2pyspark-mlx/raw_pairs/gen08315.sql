select o.status, case when o.channel in ('store', 'web') then 'core' else 'other' end as channel_group, round(avg(p.amount), 2) as avg_amount
from payments p
left outer join orders o on p.order_id = o.id
where p.order_id is null
group by o.status, case when o.channel in ('store', 'web') then 'core' else 'other' end
order by status desc, channel_group
limit 5
