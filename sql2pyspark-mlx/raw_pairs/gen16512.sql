with base as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        product_id,
        sale_date,
        revenue,
        ceil(units) as units_whole
    from store_sales
    where units <> 17
),
enriched as (
    -- Attach products attributes
    select
        src.id,
        src.store_id,
        src.product_id,
        src.revenue,
        src.units_whole,
        p.product_name
    from base src
    left join products p
        on src.product_id = p.id
),
deduped as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by store_id order by units_whole nulls first, id) as rn
    from enriched
    qualify rn = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        product_id,
        revenue,
        units_whole,
        rn,
        row_number() over (partition by product_id order by revenue, id) as event_seq,
        percent_rank() over (order by revenue desc nulls first) as revenue_percent_rank,
        ntile(4) over w_running as revenue_bucket,
        sum(revenue) over (partition by store_id, product_id order by rn, id rows between unbounded preceding and current row) as running_revenue,
        avg(units_whole) over (partition by store_id order by units_whole, id rows between 6 preceding and current row) as units_whole_avg_6
    from deduped
    where product_id = 9
    window w_running as (partition by store_id order by revenue desc, id)
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        store_id,
        units_whole,
        rn,
        revenue_percent_rank,
        revenue_bucket,
        units_whole_avg_6,
        row_number() over (order by revenue_percent_rank desc, id) as seq_no,
        round(running_revenue / sum(running_revenue) over (partition by store_id), 4) as running_revenue_share
    from metrics
)
-- Grouped metrics
select
    case when running_revenue_share >= 0.86 then 'PREMIUM' else 'STANDARD' end as running_revenue_share_tier,
    round(avg(units_whole), 2) as avg_units_whole,
    count(*) as num_rows,
    avg(units_whole) as avg_units_whole_2
from with_windows
group by running_revenue_share_tier;
