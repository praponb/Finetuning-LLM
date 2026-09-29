with scored as (
    -- Running and moving aggregates
    select
        *,
        lag(amount, 1) over (partition by merchant order by txn_time, id) as prev_amount,
        round(amount / sum(amount) over (partition by account_id, txn_type), 4) as amount_share
    from transactions
    where merchant is not null
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by account_id order by txn_time desc nulls last, id) as latest_rank
        from scored
    ) t
    where latest_rank = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval 14 days then 1 else 0 end as is_new_burst
    from ranked
),
sessions_transactions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from gaps
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_time,
        merchant,
        is_flagged,
        prev_amount,
        amount_share,
        is_new_burst,
        sum(prev_amount) over (partition by account_id order by amount_share, id rows between unbounded preceding and current row) as cumulative_prev_amount,
        avg(is_new_burst) over (partition by to_date(txn_time), txn_type order by amount_share, id rows between 6 preceding and current row) as is_new_burst_avg_6
    from sessions_transactions
),
enriched as (
    -- Attach accounts attributes
    select
        s.*,
        d.opened_date,
        d.status,
        d.balance
    from metrics s
    left join accounts d
        on s.account_id = d.id
)
-- Window metrics over several different partitions
select
    id,
    account_id,
    txn_time,
    merchant,
    is_flagged,
    cumulative_prev_amount,
    opened_date,
    status,
    stddev_samp(is_new_burst) over (partition by account_id, status) as account_id_stddev_samp_is_new_burst,
    round(cumulative_prev_amount / sum(cumulative_prev_amount) over (partition by merchant), 4) as cumulative_prev_amount_share,
    avg(amount_share) over (partition by account_id order by prev_amount, id rows between 3 preceding and current row) as amount_share_avg_3
from enriched;
