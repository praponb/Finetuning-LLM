select
    case when status in ('CANCELLED', 'RETURNED') then 'group_a' else 'other' end as status_group,
    channel,
    max(shipping_fee) - min(shipping_fee) as shipping_fee_range,
    max(amount) - min(amount) as amount_range
from orders
where shipping_fee < 13
group by status_group, channel
having max(shipping_fee) - min(shipping_fee) > 22
order by 1 desc, channel
