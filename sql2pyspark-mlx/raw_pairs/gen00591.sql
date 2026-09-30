with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        unit_price,
        coalesce(unit_price, 0) as unit_price_filled,
        cast(unit_price as int) as unit_price_int
    from order_items
    where product_id is not null
      and order_id = 9
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, rank() over (partition by order_id order by unit_price_filled desc) as latest_rank
    from filtered
    qualify latest_rank = 1
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        unit_price,
        unit_price_filled,
        unit_price_int,
        round(unit_price * 1.07, 2) as unit_price_adjusted
    from deduped
    where order_id in (11, 13, 32)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        unit_price_adjusted,
        sum(unit_price) over (partition by order_id order by unit_price_int, id rows between unbounded preceding and current row) as cumulative_unit_price,
        avg(unit_price_adjusted) over (partition by order_id) as order_id_avg_unit_price_adjusted,
        lag(unit_price_int, 2) over (order by unit_price_adjusted, id) as prev_unit_price_int
    from cleaned
),
joined as (
    -- Attach orders attributes
    select
        a.*,
        d.channel,
        d.order_date,
        d.amount
    from enriched a
    left join orders d
        on a.order_id = d.id
),
totals as (
    -- Multi-level aggregation
    select
        case when cumulative_unit_price >= 1612 then 'PREMIUM' else 'STANDARD' end as cumulative_unit_price_tier,
        date_trunc('week', order_date) as week_start,
        avg(prev_unit_price_int) filter (where channel = 'web') as web_avg_prev_unit_price_int,
        count(distinct channel) as distinct_channels
    from joined
    group by case when cumulative_unit_price >= 1612 then 'PREMIUM' else 'STANDARD' end, date_trunc('week', order_date)
)
select
    cumulative_unit_price_tier,
    week_start,
    web_avg_prev_unit_price_int
from totals
order by week_start desc, web_avg_prev_unit_price_int
