select case when channel in ('partner', 'web') then 'priority' else 'other' end as channel_group, max(order_date) as latest_order_date from orders group by channel_group
