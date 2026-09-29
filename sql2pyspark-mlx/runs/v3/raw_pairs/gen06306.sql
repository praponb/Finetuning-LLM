select
    case when o.channel in ('mobile', 'store') then 'group_a' else 'other' end as channel_group,
    o.channel,
    max(p.amount) - min(p.amount) as amount_range,
    sort_array(collect_set(p.method)) as method_list
from payments p
inner join orders o on o.id = p.order_id
group by case when o.channel in ('mobile', 'store') then 'group_a' else 'other' end, o.channel
having max(p.amount) - min(p.amount) > 42
order by channel_group desc, amount_range desc, channel
limit 5
