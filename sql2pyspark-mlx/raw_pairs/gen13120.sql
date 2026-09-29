with with_prev as (
    -- Previous event time per key
    select *, lag(txn_time) over (partition by account_id order by txn_time, id) as prev_txn_time
    from transactions
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval 14 days then 1 else 0 end as new_group_flag
    from with_prev
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
),
with_dims as (
    -- Attach accounts attributes
    select
        s.*,
        d.balance,
        d.opened_date
    from sessionized s
    join accounts d
        on s.account_id = d.id
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(new_group_flag * 0.9, 2) as new_group_flag_scaled,
        island_id - balance as island_id_minus_balance,
        coalesce(merchant, 'NONE') as merchant_clean
    from with_dims
    where merchant is not null
      and balance >= 65000
),
scored as (
    -- Ranking and gap features
    select
        *,
        min(balance) over (partition by merchant, account_id order by txn_time, id rows between unbounded preceding and current row) as running_min_balance,
        avg(balance) over (partition by merchant, account_id order by txn_time, id rows between unbounded preceding and current row) as running_avg_balance,
        max(balance) over (partition by merchant, account_id order by txn_time, id rows between unbounded preceding and current row) as running_max_balance,
        first_value(balance) ignore nulls over (partition by month(txn_time), merchant order by balance, id rows between unbounded preceding and current row) as first_known_balance
    from cleaned
    where txn_time >= '2025-04-01'
)
select
    s.id,
    s.account_id,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.prev_txn_time,
    s.balance,
    s.opened_date,
    s.island_id_minus_balance,
    (select avg(t2.amount) from transactions t2 where t2.account_id = s.account_id) as avg_transaction_amount
from scored s;
