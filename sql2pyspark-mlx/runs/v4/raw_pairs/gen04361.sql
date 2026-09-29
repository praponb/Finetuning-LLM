with enriched as (
    -- Attach accounts attributes
    select
        a.*,
        b.opened_date,
        b.balance,
        b.status
    from transactions a
    left join accounts b
        on a.account_id = b.id
),
with_dims as (
    -- Attach accounts attributes
    select e.*, a.opened_date as account_opened_date
    from enriched e
    left join accounts a
        on e.account_id = a.id
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by txn_type, merchant order by opened_date asc, id asc) as rank_in_group
    from with_dims
    qualify rank_in_group = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(txn_time) over (partition by status order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by status order by txn_time asc, id asc) + interval '60' day then 1 else 0 end as new_group_flag
    from deduped
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by txn_time asc, id asc rows between unbounded preceding and current row) as island_id
    from gaps
),
summary as (
    -- Aggregate with conditional metrics
    select
        case when balance >= 80000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
        count(*) filter (where txn_type = 'DEBIT') as debit_rows,
        (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as duration_min,
        sum(island_id) filter (where status = 'FROZEN') as frozen_total_island_id,
        min(txn_time) as first_seen
    from sessions_transactions
    group by case when balance >= 80000 then 'PREMIUM' else 'STANDARD' end
)
select
    balance_tier,
    debit_rows,
    duration_min,
    first_seen
from summary
order by debit_rows asc, duration_min asc;
