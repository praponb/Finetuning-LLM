with valid as (
    -- Filter the raw rows and derive helper columns
    select *, case when status in ('IN_TRANSIT', 'LOST') then 'Core' else 'Other' end as status_group
    from shipments
    where carrier is distinct from 'Kerry'
      and order_id is null
),
joined as (
    -- Attach orders attributes
    select e.*, p.status as order_status
    from valid e
    left join orders p
        on e.order_id <=> p.id
),
enriched as (
    -- Ranking and gap features
    select
        *,
        avg(cost) over w_running as last3_avg_cost,
        sum(cost) over w_running as last3_sum_cost,
        max(cost) over w_running as last3_max_cost
    from joined
    window w_running as (partition by order_id order by delivered_date, id rows between 2 preceding and current row)
),
filtered as (
    -- Basic cleansing and defaults
    select
        *,
        last3_sum_cost + cost as last3_sum_cost_plus_cost,
        coalesce(order_status, 'DIRECT') as order_status_filled
    from enriched
    where shipped_date >= date '2023-04-28'
      and carrier is not distinct from 'DHL'
)
-- Multi-level aggregation
select
    case when cost >= 77 then 'PREMIUM' else 'STANDARD' end as cost_tier,
    avg(last3_sum_cost_plus_cost) as avg_last3_sum_cost_plus_cost,
    percentile_approx(cost, array(0.25, 0.5, 0.75)) as cost_quartiles,
    count_if(order_status = 'CANCELLED') as cancelled_events
from filtered
group by cost_tier
order by avg_last3_sum_cost_plus_cost desc
