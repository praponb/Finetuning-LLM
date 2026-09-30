with base as (
    -- Keep recent, valid rows
    select
        *,
        length(txn_type) as txn_type_length,
        cast(amount as int) as amount_int
    from transactions
    where txn_time >= date '2023-08-29'
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, date_format(txn_time, 'yyyy-MM') as txn_time_label
    from base
    where txn_time >= timestamp '2023-07-18 13:00:00'
      and txn_type = 'DEBIT'
),
base_transactions as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        amount,
        is_flagged,
        txn_type_length,
        txn_time_label,
        txn_type_length * amount as txn_type_length_times_amount,
        cast(txn_time as date) as txn_time_day
    from filtered
    where txn_time > timestamp '2023-09-06 11:00:00'
      and merchant is distinct from 'Lazada'
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by account_id order by txn_type_length_times_amount, id) as latest_rank
    from base_transactions
    qualify latest_rank = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        account_id,
        is_flagged,
        txn_type_length,
        txn_type_length_times_amount,
        txn_time_day,
        latest_rank,
        ntile(4) over (order by amount desc, id) as amount_bucket,
        avg(amount) over (order by latest_rank, id rows between 6 preceding and 1 preceding) as amount_avg_6_prior,
        ntile(10) over (partition by account_id order by txn_type_length_times_amount desc, id) as txn_type_length_times_amount_bucket,
        max(amount) over (partition by account_id) as account_id_max_amount
    from ranked
)
-- Ranking and gap features
select
    *,
    lead(amount_avg_6_prior) over w_running as next_amount_avg_6_prior,
    lead(txn_type_length) over w_running as next_txn_type_length,
    lag(txn_type_length) over w_running as prev_txn_type_length,
    row_number() over w_running as seq_no
from scored
window w_running as (partition by account_id order by latest_rank, id)
