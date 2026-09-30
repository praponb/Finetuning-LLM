with base as (
    select *
    from orders
    where amount <= 1270
),
orders_stats as (
    select channel, status, round((100 * sum(case when channel = 'web' then 1 else 0 end)) / count(*), 1) as pct_web
    from base
    group by channel, status
)
select channel, status, pct_web
from orders_stats
order by pct_web;
