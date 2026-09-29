select id, customer_id, opened_date, ceil(balance) as balance_whole, case when status in ('OPEN', 'CLOSED') then 'Tier 1' else 'Other' end as status_group from accounts order by 1
