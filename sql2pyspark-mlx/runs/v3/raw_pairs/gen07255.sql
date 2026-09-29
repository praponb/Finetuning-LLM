select order_month, channel, pct_pending
from (
    select month(order_date) order_month, channel, round((100 * sum(case when status = 'PENDING' then 1 else 0 end)) / count(*), 1) pct_pending
    from orders
    group by month(order_date), channel
) agg
where pct_pending < 40;
