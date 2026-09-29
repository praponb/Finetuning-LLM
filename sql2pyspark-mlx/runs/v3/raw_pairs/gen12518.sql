select amount, case when channel in ('web', 'mobile') then 'Core' else 'Other' end as channel_group from orders where order_date >= '2025-07-01' order by channel_group desc
