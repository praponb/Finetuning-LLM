with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        case when txn_type in ('REFUND', 'DEBIT') then 'Group A' else 'Other' end as txn_type_group
    from transactions
    where txn_type = 'DEBIT'
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval 60 days then 1 else 0 end as session_start_flag
    from cleaned
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as session_id
    from flagged
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        session_id,
        datediff(current_date(), txn_time) as days_since_txn_time,
        round(session_id, 0) as session_id_rounded
    from sessionized
    where session_id between 4 and 7
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(amount) over (partition by txn_type order by session_id, id rows between 2 preceding and current row) as amount_avg_2,
        avg(amount) over (partition by txn_type order by cast(txn_time as bigint) range between 7776000 preceding and current row) as amount_avg_90d
    from base
    where days_since_txn_time <> 32
),
remaining_keys as (
    -- Set difference on keys
    select id, txn_type
    from metrics
    except
    select id, txn_type
    from transactions
    where amount < 1400
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join remaining_keys k on s.id = k.id and s.txn_type = k.txn_type
),
scored as (
    -- Running and moving aggregates
    select *, round(amount / sum(amount) over (partition by year(txn_time), txn_type), 4) as amount_share
    from reconciled
),
metrics_transactions as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over (partition by txn_type order by session_id desc) as session_id_percent_rank,
        max(amount_avg_2) over (order by cast(txn_time as bigint) range between 2592000 preceding and current row) as amount_avg_2_max_30d,
        round(amount_avg_2 / sum(amount_avg_2) over (partition by txn_type), 4) as amount_avg_2_share
    from scored
)
-- Aggregate with conditional metrics
select
    txn_type,
    count(*) filter (where txn_type = 'CREDIT') as credit_rows,
    round(sum(amount_avg_2) / nullif(count(case when txn_type = 'DEBIT' then 1 end), 0), 2) as amount_avg_2_per_debit,
    min(txn_time) as first_txn_time
from metrics_transactions
group by cube (txn_type)
order by amount_avg_2_per_debit nulls first;
