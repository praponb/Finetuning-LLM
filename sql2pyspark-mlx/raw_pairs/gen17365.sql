with base as (
    -- Filter the raw rows and derive helper columns
    select *, trim(txn_type) as clean_txn_type
    from transactions
    where merchant is not null
      and is_flagged
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        clean_txn_type,
        lead(amount, 2) over (order by txn_time, id) as next_amount,
        row_number() over w_grp as seq_no,
        row_number() over (partition by account_id order by txn_time, id) as event_seq,
        round(amount / sum(amount) over (partition by to_date(txn_time)), 4) as amount_share
    from base
    window w_grp as (partition by account_id order by amount desc, id)
),
joined as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.amount,
        s.txn_time,
        s.is_flagged,
        s.next_amount,
        s.event_seq,
        s.amount_share,
        b.status
    from with_windows s
    left join accounts b
        on s.account_id = b.id
    where b.account_type != 'LOAN'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by account_id, status order by txn_time desc nulls last, id) as rank_in_group
    from joined
    qualify rank_in_group = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by status order by txn_time, id) > interval 7 days then 1 else 0 end as session_start_flag
    from latest
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        amount,
        txn_time,
        is_flagged,
        next_amount,
        amount_share,
        status,
        rank_in_group,
        sum(session_start_flag) over (partition by status order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by status order by burst_id desc, id) as rank_in_group_2
        from islands
    ) ranked
    where rank_in_group_2 = 1
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        rank_in_group * burst_id as rank_in_group_times_burst_id,
        substring(status, 1, 4) as status_prefix
    from ranked
    where txn_time >= to_timestamp('2023-10-13 05:00:00')
      and status is not null
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        status,
        amount_share,
        rank_in_group_times_burst_id
    from valid
) src
unpivot include nulls (
    metric_value for measure in (amount_share, rank_in_group_times_burst_id)
) unp
