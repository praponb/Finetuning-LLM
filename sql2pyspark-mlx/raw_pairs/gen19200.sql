with filtered as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        lower(txn_type) as txn_type_lower,
        coalesce(merchant, 'N/A') as resolved_merchant
    from transactions
    where not is_flagged
),
gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '30' day then 1 else 0 end as is_new_session
    from filtered
),
sessions_transactions as (
    select
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        txn_type_lower,
        sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
gaps_2 as (
    select *, case when lag(txn_time) over (partition by txn_type order by txn_time, id) is null
                 or txn_time > lag(txn_time) over (partition by txn_type order by txn_time, id) + interval '60' day then 1 else 0 end as session_start_flag
    from sessions_transactions
),
islands as (
    select *, sum(session_start_flag) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as burst_id_2
    from gaps_2
),
flagged as (
    select *, case when txn_time - lag(txn_time) over (partition by txn_type order by txn_time, id) > interval '90' day then 1 else 0 end as is_new_session
    from islands
),
sessions_transactions_2 as (
    select *, sum(is_new_session) over (partition by txn_type order by txn_time, id rows between unbounded preceding and current row) as island_id
    from flagged
)
select
    id,
    txn_type,
    amount,
    txn_time,
    is_flagged,
    txn_type_lower,
    burst_id,
    session_start_flag,
    burst_id_2,
    case when txn_type rlike '^(DEBIT|FEE)$' then 'PRIORITY' else 'GROUP_A' end as txn_type_zone
from sessions_transactions_2
order by id desc nulls last, txn_type_zone
limit 5;
