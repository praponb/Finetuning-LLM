with with_dims as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.txn_type,
        s.txn_time,
        s.is_flagged,
        d.opened_date,
        d.status
    from transactions s
    inner join accounts d
        on s.account_id = d.id
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 60 days then 1 else 0 end as is_new_session
    from with_dims
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
with_gaps_2 as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by status order by txn_time, id) > interval 14 days then 1 else 0 end as new_group_flag
    from islands
),
islands_transactions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by status order by txn_time, id rows between unbounded preceding and current row) as burst_id_2
    from with_gaps_2
),
metrics as (
    -- Ranking and gap features
    select
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        opened_date,
        status,
        is_new_session,
        max(burst_id_2) over (partition by account_id order by cast(txn_time as bigint) range between 2592000 preceding and current row) as burst_id_2_max_30d,
        row_number() over w_ordered as event_seq
    from islands_transactions
    window w_ordered as (partition by txn_type order by txn_time desc, id)
)
-- Ranking and gap features
select
    id,
    txn_type,
    is_flagged,
    opened_date,
    status,
    is_new_session,
    burst_id_2_max_30d,
    event_seq,
    dense_rank() over (partition by status order by burst_id_2_max_30d) as burst_id_2_max_30d_dense_rank,
    sum(event_seq) over (partition by account_id order by opened_date, id rows between unbounded preceding and current row) as running_event_seq
from metrics
where event_seq > 4;
