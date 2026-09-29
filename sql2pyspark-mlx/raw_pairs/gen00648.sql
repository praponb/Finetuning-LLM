with joined as (
    -- Attach products attributes
    select
        a.id,
        a.order_id,
        a.product_id,
        a.unit_price,
        d.stock_qty
    from order_items a
    left join products d
        on a.product_id <=> d.id
    where d.launch_date between '2024-07-01' and '2025-08-01'
),
valid as (
    -- Basic cleansing and defaults
    select *, round(unit_price, 0) as unit_price_rounded
    from joined
    where product_id is not null
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        product_id,
        unit_price,
        stock_qty,
        unit_price_rounded,
        ntile(5) over (order by unit_price_rounded, id) as unit_price_rounded_bucket,
        row_number() over (partition by order_id order by unit_price_rounded, id) as row_seq
    from valid
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        stock_qty,
        unit_price_rounded
    from enriched
    qualify row_number() over (partition by product_id order by unit_price_rounded desc, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        sum(unit_price_rounded) over w_running as running_sum_unit_price_rounded,
        max(unit_price_rounded) over w_running as running_max_unit_price_rounded,
        avg(unit_price_rounded) over w_running as running_avg_unit_price_rounded,
        row_number() over (order by stock_qty desc, id) as event_seq
    from ranked
    window w_running as (order by unit_price_rounded, id rows between unbounded preceding and current row)
)
select
    id,
    stock_qty,
    running_sum_unit_price_rounded,
    running_avg_unit_price_rounded
from metrics
order by running_sum_unit_price_rounded nulls last;
