select
    status,
    case when channel in ('mobile', 'partner') then 'group_a' else 'other' end as channel_group,
    max(order_date) as last_order_date,
    sum(discount) as total_discount,
    min(order_date) as min_order_date
from orders
where order_date <= '2024-02-01'
group by status, case when channel in ('mobile', 'partner') then 'group_a' else 'other' end
order by status desc, last_order_date
