with valid as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        sale_date,
        units,
        revenue,
        round(units / revenue, 2) as revenue_per_unit
    from store_sales
    where sale_date >= date '2023-09-12'
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue_per_unit,
        first_value(revenue, true) over w_recent as first_known_revenue,
        first_value(revenue_per_unit, true) over w_recent as first_known_revenue_per_unit,
        sum(revenue_per_unit) over (partition by product_id order by revenue_per_unit, id rows between unbounded preceding and current row) as cumulative_revenue_per_unit
    from valid
    window w_recent as (partition by product_id order by sale_date, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        product_id,
        sale_date,
        revenue_per_unit,
        cumulative_revenue_per_unit,
        round(cumulative_revenue_per_unit * 0.85, 2) as adjusted_cumulative_revenue_per_unit
    from with_windows
    where sale_date > date '2023-04-07'
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from base
    qualify row_number() over (partition by product_id order by revenue_per_unit desc, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(3) over w_hist as adjusted_cumulative_revenue_per_unit_bucket,
        lag(adjusted_cumulative_revenue_per_unit, 1, 0) over (partition by product_id order by revenue_per_unit, id) as prev_adjusted_cumulative_revenue_per_unit
    from latest
    window w_hist as (partition by product_id order by adjusted_cumulative_revenue_per_unit, id)
)
select
    s.id,
    s.product_id,
    s.sale_date,
    s.revenue_per_unit,
    s.cumulative_revenue_per_unit,
    s.adjusted_cumulative_revenue_per_unit,
    s.adjusted_cumulative_revenue_per_unit_bucket,
    s.prev_adjusted_cumulative_revenue_per_unit,
    (select max(o2.quantity) from order_items o2 where o2.product_id = s.product_id) as max_order_item_quantity
from enriched s;
