select
    oi.id,
    oi.product_id,
    o.discount,
    o.order_date,
    case when o.channel in ('partner', 'mobile') then 'Group A' else 'Other' end as channel_group
from order_items oi
inner join orders o on oi.order_id = o.id
order by o.discount desc, channel_group desc;
