with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        date_sub(shipped_date, 57) as shipped_date_minus_57d,
        coalesce(carrier, 'UNKNOWN') as carrier_filled
    from shipments
    where order_id is not null
),
valid_shipments as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        coalesce(cost, 0) as cost_filled
    from valid
    where carrier is distinct from 'Flash'
      and shipped_date between '2023-10-01' and '2025-08-01'
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by order_id order by cost_filled desc) as cost_filled_rank,
        ntile(4) over w_grp as cost_bucket
    from valid_shipments
    window w_grp as (partition by carrier order by cost desc, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        carrier,
        shipped_date,
        cost_filled,
        cost_bucket
    from with_windows
    qualify rank() over (partition by status order by shipped_date nulls first) = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        avg(cost_bucket) over w_ordered as last7_avg_cost_bucket,
        sum(cost_bucket) over w_ordered as last7_sum_cost_bucket,
        max(cost_bucket) over w_ordered as last7_max_cost_bucket,
        min(cost_bucket) over w_ordered as last7_min_cost_bucket
    from latest
    window w_ordered as (partition by carrier order by shipped_date, id rows between 6 preceding and current row)
)
-- Multi-level aggregation
select
    case when last7_avg_cost_bucket >= 6.6 then 'LARGE' else 'SMALL' end as last7_avg_cost_bucket_tier,
    count(*) as num_rows,
    count(*) filter (where carrier = 'UPS') as ups_rows,
    max(last7_avg_cost_bucket) filter (where carrier = 'Flash') as flash_max_last7_avg_cost_bucket,
    size(collect_set(carrier)) as num_carriers,
    avg(last7_min_cost_bucket) as avg_last7_min_cost_bucket,
    avg(cost_bucket) as avg_cost_bucket
from scored
group by 1
order by avg_cost_bucket;
