with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '30' day then 1 else 0 end as session_start_flag
    from transactions
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Multi-level aggregation
select
    date_trunc('week', txn_time) as week_start,
    size(collect_set(merchant)) as num_merchants,
    round(sum(session_start_flag) / nullif(count(case when merchant = 'Starbucks' then 1 end), 0), 2) as session_start_flag_per_starbucks,
    count(*) filter (where txn_type = 'FEE') as fee_rows
from sessions_transactions
group by 1
order by fee_rows desc nulls last;
