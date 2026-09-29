with base as (
    -- Basic cleansing and defaults
    select
        id,
        store_id,
        sale_date,
        units,
        revenue,
        date_trunc('WEEK', sale_date) sale_date_period,
        cast(units as string) units_num
    from store_sales
    where sale_date > to_date('2023-03-20')
),
cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        cast(units as double) units_num_2,
        ceil(revenue) revenue_whole
    from base
    where sale_date > to_date('2023-06-01')
      and revenue >= 2000
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(sale_date_period) over (partition by store_id order by sale_date_period, id) is null
                 or sale_date_period > lag(sale_date_period) over (partition by store_id order by sale_date_period, id) + interval 30 days then 1 else 0 end as new_group_flag
    from cleaned
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by store_id order by sale_date_period, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (partition by store_id order by new_group_flag nulls last) new_group_flag_cume_dist,
        row_number() over (partition by store_id order by sale_date_period desc, id) event_seq,
        lag(revenue, 1) over (partition by store_id order by island_id, id) prev_revenue
    from islands
),
grouped as (
    -- Multi-level aggregation
    select
        date_trunc('week', sale_date_period) as week_start,
        date_trunc('quarter', sale_date_period) as quarter_start,
        percentile_approx(new_group_flag_cume_dist, array(0.25, 0.5, 0.75)) as new_group_flag_cume_dist_quartiles,
        count(*) as event_count,
        (unix_timestamp(max(sale_date_period)) - unix_timestamp(min(sale_date_period))) / 60 as duration_min,
        (unix_timestamp(max(sale_date_period)) - unix_timestamp(min(sale_date_period))) / 60 as duration_min_2,
        count(distinct store_id) as distinct_store_ids,
        rank() over (partition by date_trunc('week', sale_date_period) order by count(*) desc) as event_count_rank
    from scored
    group by 1, 2
)
select
    week_start,
    quarter_start,
    duration_min,
    distinct_store_ids,
    event_count_rank
from grouped
order by duration_min desc nulls first
