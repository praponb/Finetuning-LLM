with base as (
    select
        id,
        customer_id,
        status,
        case when balance >= 61000 then 'High' when balance >= 14000 then 'Medium' else 'Low' end as balance_band,
        coalesce(status, 'N/A') as resolved_status
    from accounts
    where opened_date >= date '2023-12-15'
),
latest_match as (
    select s.*, latest.transaction_txn_type
    from base s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
filtered as (
    select
        *,
        coalesce(transaction_txn_type, 'unassigned') as transaction_txn_type_filled,
        coalesce(status, 'DIRECT') as status_filled
    from latest_match
    where transaction_txn_type in ('CREDIT', 'FEE')
),
filtered_accounts as (
    select
        id,
        customer_id,
        status,
        balance_band,
        transaction_txn_type,
        status_filled,
        lower(status) as status_lower,
        coalesce(transaction_txn_type, 'Unknown') as transaction_txn_type_filled_2
    from filtered
    where customer_id in (19, 21, 33)
),
with_latest as (
    select s.*, x.transaction_txn_time
    from filtered_accounts s
    join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
metrics as (
    select
        *,
        last_value(transaction_txn_type) ignore nulls over (partition by transaction_txn_type order by transaction_txn_time, id rows between unbounded preceding and current row) as last_known_transaction_txn_type,
        first_value(status) ignore nulls over (partition by customer_id order by transaction_txn_time, id rows between unbounded preceding and current row) as first_known_status
    from with_latest
),
with_gaps as (
    select *, case when lag(transaction_txn_time) over (partition by transaction_txn_type order by transaction_txn_time, id) is null
                 or transaction_txn_time > lag(transaction_txn_time) over (partition by transaction_txn_type order by transaction_txn_time, id) + interval '14' day then 1 else 0 end as session_start_flag
    from metrics
),
sessionized as (
    select *, sum(session_start_flag) over (partition by transaction_txn_type order by transaction_txn_time, id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
select
    id,
    balance_band,
    status_filled,
    status_lower,
    transaction_txn_time,
    last_known_transaction_txn_type,
    first_known_status,
    session_start_flag,
    session_id
from sessionized;
