with base as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        sale_date,
        coalesce(revenue, 0) as revenue_filled
    from store_sales
    where sale_date >= to_date('2023-10-01')
      and store_id in (3, 5, 6)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        revenue_filled,
        row_number() over (partition by store_id order by revenue_filled, id) as seq_no,
        sum(revenue_filled) over w_recent as running_revenue_filled,
        last_value(revenue_filled, true) over (partition by store_id order by sale_date, id rows between unbounded preceding and current row) as last_known_revenue_filled
    from base
    where revenue_filled >= 27
    window w_recent as (partition by month(sale_date), store_id order by sale_date, id rows between unbounded preceding and current row)
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        last_value(revenue_filled) ignore nulls over w_grp as last_known_revenue_filled_2,
        stddev_samp(last_known_revenue_filled) over w_part as store_id_stddev_samp_last_known_revenue_filled,
        rank() over (partition by store_id order by seq_no desc nulls last) as seq_no_rank,
        rank() over (partition by store_id order by revenue_filled) as revenue_filled_rank,
        lag(last_known_revenue_filled, 1) over w_ordered as prev_last_known_revenue_filled
    from with_windows
    window w_grp as (partition by store_id order by last_known_revenue_filled, id rows between unbounded preceding and current row), w_part as (partition by store_id), w_ordered as (partition by store_id order by running_revenue_filled, id)
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from enriched
    qualify row_number() over (partition by store_id order by seq_no desc nulls last, id) = 1
),
with_dims as (
    -- Attach stores attributes
    select
        s.*,
        p.city,
        p.opened_date
    from latest s
    inner join stores p
        on s.store_id = p.id
),
enriched_store_sales as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(prev_last_known_revenue_filled) over w_ordered as city_stddev_samp_prev_last_known_revenue_filled,
        round(seq_no / sum(seq_no) over (partition by city), 4) as seq_no_share,
        ntile(3) over (partition by store_id order by revenue_filled desc, id) as revenue_filled_bucket
    from with_dims
    window w_ordered as (partition by city)
),
totals as (
    -- Grouped metrics
    select
        city,
        store_id,
        sum(case when city = 'Berlin' then last_known_revenue_filled else 0 end) as berlin_last_known_revenue_filled,
        size(collect_set(city)) as num_cities,
        sum(case when city = 'Berlin' then last_known_revenue_filled_2 else 0 end) as berlin_last_known_revenue_filled_2,
        grouping_id() as gid
    from enriched_store_sales
    group by rollup (city, store_id)
)
select city, berlin_last_known_revenue_filled_2
from totals
order by berlin_last_known_revenue_filled_2 nulls last, city desc
