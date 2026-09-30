select amount, shipping_fee, channel, case when status in ('PENDING', 'RETURNED') then 'Group A' else 'Other' end as status_group from orders;
