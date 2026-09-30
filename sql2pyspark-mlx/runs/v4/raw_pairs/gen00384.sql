with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        round(cost, 2) as cost_rounded,
        coalesce(cost, 0) as cost_filled
    from shipments
    where carrier is not null
      and carrier in ('Flash', 'UPS', 'DHL')
)
-- Running and moving aggregates
select
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    cost_rounded,
    cost_filled,
    sum(cost_rounded) over (partition by status order by shipped_date, id rows between unbounded preceding and current row) as running_cost_rounded,
    cume_dist() over (order by cost nulls first) as cost_cume_dist,
    lag(cost_rounded, 1, 0) over (partition by order_id, status order by delivered_date, id) as prev_cost_rounded,
    lead(cost_rounded, 2) over (partition by status order by cost_rounded, id) as next_cost_rounded,
    ntile(3) over (partition by order_id order by cost desc, id) as cost_bucket
from filtered
