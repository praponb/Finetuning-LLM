with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        revenue * units as revenue_per_unit
    from store_sales
    where sale_date >= date '2023-05-07'
      and revenue < 1600
),
base_store_sales as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_id,
        product_id,
        revenue,
        revenue_per_unit,
        revenue_per_unit * units as revenue_per_unit_times_units
    from base
    where sale_date >= date '2023-03-20'
      and revenue_per_unit between 46 and 75
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by store_id order by revenue desc nulls last, id) as latest_rank
        from base_store_sales
    ) t
    where latest_rank = 1
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        revenue,
        revenue_per_unit,
        revenue_per_unit_times_units,
        latest_rank,
        lead(revenue) over (partition by store_id, product_id order by revenue, id) as next_revenue,
        lag(revenue_per_unit_times_units) over (partition by store_id, product_id order by revenue, id) as prev_revenue_per_unit_times_units,
        lead(revenue_per_unit_times_units) over (partition by store_id, product_id order by revenue, id) as next_revenue_per_unit_times_units,
        row_number() over (partition by store_id, product_id order by revenue, id) as event_seq
    from latest
    where product_id = 4
),
kept_keys as (
    -- Set intersection on keys
    select store_id, id
    from with_windows
    intersect all
    select store_id, id
    from store_sales
    where revenue is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    inner join kept_keys k on s.store_id = k.store_id and s.id = k.id
)
-- Multi-level aggregation
select
    store_id,
    case when next_revenue >= 3500 then 'HIGH' else 'LOW' end as next_revenue_tier,
    approx_percentile(next_revenue_per_unit_times_units, 0.5) as next_revenue_per_unit_times_units_p50,
    count(distinct id) as distinct_ids,
    percentile_approx(revenue_per_unit, array(0.25, 0.5, 0.75)) as revenue_per_unit_quartiles
from reconciled
group by rollup (store_id, case when next_revenue >= 3500 then 'HIGH' else 'LOW' end)
order by store_id
