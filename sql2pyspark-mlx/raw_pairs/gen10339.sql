with metrics as (
    -- Window metrics over several different partitions
    select
        *,
        lag(cost) over w_grp as prev_cost,
        lead(cost) over w_grp as next_cost,
        row_number() over w_grp as event_seq
    from shipments
    where order_id is null
    window w_grp as (partition by carrier order by shipped_date asc, id asc)
),
metrics_shipments as (
    -- Ranking and gap features
    select
        id,
        carrier,
        shipped_date,
        event_seq,
        ntile(10) over (partition by carrier order by prev_cost asc, id asc) as prev_cost_bucket,
        sum(prev_cost) over (partition by year(shipped_date), order_id order by prev_cost asc, id asc rows between unbounded preceding and current row) as cumulative_prev_cost
    from metrics
),
matched_keys as (
    -- Set difference on keys
    select id, carrier
    from metrics_shipments
    except
    select id, carrier
    from shipments
    where shipped_date < '2024-03-30'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics_shipments s
    inner join matched_keys k on s.id = k.id and s.carrier = k.carrier
),
matched_keys_2 as (
    -- Set intersection on keys
    select id, carrier
    from kept_rows
    intersect all
    select id, carrier
    from shipments
    where order_id is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept_rows s
    inner join matched_keys_2 k on s.id = k.id and s.carrier = k.carrier
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        carrier,
        shipped_date,
        event_seq,
        prev_cost_bucket,
        cumulative_prev_cost,
        round(cumulative_prev_cost / event_seq, 2) as cumulative_prev_cost_per_event_seq,
        least(cumulative_prev_cost, prev_cost_bucket) as least_cumulative_prev_cost_prev_cost_bucket
    from reconciled
    where carrier is not null
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        shipped_date,
        prev_cost_bucket,
        cumulative_prev_cost,
        cumulative_prev_cost_per_event_seq,
        greatest(prev_cost_bucket, least_cumulative_prev_cost_prev_cost_bucket) as greatest_prev_cost_bucket_least_cumulative_prev_cost_prev_cost_bucket,
        round(prev_cost_bucket / cumulative_prev_cost, 2) as prev_cost_bucket_per_cumulative_prev_cost,
        coalesce(carrier, 'UNKNOWN') as carrier_filled
    from cleaned
    where shipped_date > to_date('2023-06-01')
      and carrier is not null
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        shipped_date,
        cumulative_prev_cost,
        cumulative_prev_cost_per_event_seq,
        greatest_prev_cost_bucket_least_cumulative_prev_cost_prev_cost_bucket,
        carrier_filled,
        round(prev_cost_bucket, 1) as prev_cost_bucket_rounded,
        add_months(shipped_date, 3) as shipped_date_plus_months
    from valid
    where cumulative_prev_cost_per_event_seq <= 51
)
-- Multi-level aggregation
select
    date_trunc('month', shipped_date_plus_months) as activity_month,
    count(*) as cnt,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    count(distinct id) as distinct_ids_3
from base
group by date_trunc('month', shipped_date_plus_months)
order by distinct_ids asc nulls first
