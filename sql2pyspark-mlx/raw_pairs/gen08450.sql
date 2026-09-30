with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when amount >= 1740 then 'High' when amount >= 290 then 'Medium' else 'Low' end as amount_tier,
        coalesce(channel, 'DIRECT') as resolved_channel
    from orders
    where channel is distinct from 'store'
      and customer_id is not null
)
-- Running and moving aggregates
select
    id,
    customer_id,
    store_id,
    status,
    channel,
    amount_tier,
    rank() over w_running as discount_rank,
    round(amount / sum(amount) over (partition by status), 4) as amount_share,
    percent_rank() over (partition by status order by shipping_fee desc nulls first) as shipping_fee_percent_rank
from base
window w_running as (order by discount asc nulls first);
