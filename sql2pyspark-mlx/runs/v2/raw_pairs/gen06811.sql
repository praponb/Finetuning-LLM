with recent_orders as (
    select *
    from orders
    where store_id is not null and store_id in (13, 23, 26) and month(order_date) in (1, 7, 11)
),
Summary as (
    select status, sum(case when channel = 'partner' then 1 else 0 end) as partner_count
    from recent_orders
    group by status
)
select status, partner_count
from Summary
where partner_count < 39
order by status
