with valid as (
    select *, round(revenue * 1.1, 2) as revenue_adjusted
    from store_sales
    where sale_date > date '2023-05-21'
),
deduped as (
    select
        id,
        store_id,
        units,
        revenue,
        revenue_adjusted
    from valid
    qualify rank() over (partition by store_id, product_id order by revenue desc nulls last) = 1
),
cleaned as (
    select
        id,
        store_id,
        units,
        revenue,
        revenue_adjusted,
        revenue_adjusted - units as revenue_adjusted_minus_units
    from deduped
    where revenue <= 2700
),
scored as (
    select
        id,
        revenue,
        lag(units) over (partition by store_id order by revenue_adjusted asc, id asc) as prev_units,
        lag(revenue) over (partition by store_id order by revenue_adjusted asc, id asc) as prev_revenue,
        lead(revenue) over (partition by store_id order by revenue_adjusted asc, id asc) as next_revenue,
        row_number() over (partition by store_id order by revenue_adjusted asc, id asc) as seq_no,
        lead(units, 1) over (order by units asc, id asc) as next_units
    from cleaned
),
base as (
    select
        id,
        revenue,
        prev_units,
        prev_revenue,
        next_revenue,
        coalesce(prev_revenue, 0) as prev_revenue_filled,
        cast(next_units as string) as next_units_num
    from scored
    where next_units is not null
),
metrics as (
    select
        *,
        lead(prev_units) over w_hist as next_prev_units,
        lag(prev_units) over w_hist as prev_prev_units,
        lag(prev_revenue) over w_hist as prev_prev_revenue,
        lead(prev_revenue) over w_hist as next_prev_revenue,
        row_number() over w_hist as event_seq
    from base
    where prev_revenue <= 1800
    window w_hist as (order by revenue asc, id asc)
)
select
    id,
    prev_units,
    next_revenue,
    prev_revenue_filled,
    next_prev_units,
    prev_prev_revenue,
    next_prev_revenue,
    event_seq
from metrics
order by prev_units asc, prev_revenue_filled desc
