with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        avg(amount) over (partition by status order by cast(paid_at as bigint) range between 2592000 preceding and current row) as amount_avg_30d,
        ntile(3) over (order by amount, id) as amount_bucket,
        first_value(method, true) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as first_known_method
    from payments
    where status <> 'REFUNDED'
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        paid_at,
        status,
        amount_avg_30d,
        amount_bucket,
        first_known_method
    from with_windows
    qualify row_number() over (partition by order_id order by paid_at desc, id) = 1
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 14 days then 1 else 0 end as new_group_flag
    from deduped
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_no
    from flagged
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when paid_at - lag(paid_at) over (partition by status order by paid_at, id) > interval 14 days then 1 else 0 end as session_start_flag
    from sessionized
),
sessions_payments as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    case when amount_avg_30d >= 1305 then 'HIGH' else 'LOW' end as amount_avg_30d_tier,
    case when amount_bucket >= 9 then 'PREMIUM' else 'STANDARD' end as amount_bucket_tier,
    date_trunc('year', paid_at) as year_start,
    max(session_no) filter (where status = 'REFUNDED') as refunded_max_session_no,
    count(distinct status) as distinct_status,
    sort_array(collect_set(status)) as status_list
from sessions_payments
group by case when amount_avg_30d >= 1305 then 'HIGH' else 'LOW' end, case when amount_bucket >= 9 then 'PREMIUM' else 'STANDARD' end, date_trunc('year', paid_at)
order by amount_avg_30d_tier nulls first
