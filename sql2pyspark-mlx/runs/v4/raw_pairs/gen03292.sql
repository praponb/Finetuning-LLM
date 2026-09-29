with src as (
    select *
    from orders
    where channel = 'partner'
),
summary as (
    select channel, sum(case when status = 'SHIPPED' then 1 else 0 end) shipped_count, sum(discount) total_discount
    from src
    group by channel
)
select *
from summary
where total_discount > 2.2
order by shipped_count desc, channel
limit 10;
