with base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        cast(is_flagged as int) as is_flagged_flag,
        coalesce(merchant, 'NONE') as resolved_merchant
    from transactions
    where txn_time > to_date('2023-08-07')
      and merchant is not null
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '7' day then 1 else 0 end as session_start_flag
    from base
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Aggregate with conditional metrics
select
    case when island_id >= 8 then 'HIGH' else 'LOW' end as island_id_tier,
    date_trunc('quarter', txn_time) as quarter_start,
    case when amount >= 4200 then 'LARGE' else 'SMALL' end as amount_tier,
    round(avg(is_flagged_flag), 2) as avg_is_flagged_flag,
    count(*) as num_rows,
    round(avg(session_start_flag), 2) as avg_session_start_flag,
    approx_percentile(is_flagged_flag, 0.5) as is_flagged_flag_p50,
    round(sum(is_flagged_flag) / nullif(count(case when txn_type = 'DEBIT' then 1 end), 0), 2) as is_flagged_flag_per_debit,
    min(session_start_flag) as min_session_start_flag
from sessions_transactions
group by case when island_id >= 8 then 'HIGH' else 'LOW' end, date_trunc('quarter', txn_time), case when amount >= 4200 then 'LARGE' else 'SMALL' end
order by avg_is_flagged_flag;
