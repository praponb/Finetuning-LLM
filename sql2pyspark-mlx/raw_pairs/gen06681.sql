with filtered as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        amount,
        merchant,
        to_date(txn_time) txn_time_date,
        to_date(txn_time) txn_time_date_2,
        coalesce(merchant, 'N/A') merchant_filled
    from transactions
    where txn_type in ('FEE', 'DEBIT')
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from filtered
    qualify row_number() over (partition by merchant, txn_type order by txn_time_date_2 desc nulls last, id asc) = 1
),
filtered_transactions as (
    -- Basic cleansing and defaults
    select
        *,
        coalesce(amount, 0) amount_filled,
        round(amount, 1) amount_rounded
    from ranked
    where merchant is not null
      and merchant is distinct from 'Grab'
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        min(amount_filled) over (partition by merchant order by amount asc, id asc rows between 6 preceding and current row) last7_min_amount_filled,
        count(amount_filled) over (partition by merchant order by amount asc, id asc rows between 6 preceding and current row) last7_count_amount_filled,
        avg(amount_filled) over (partition by merchant order by amount asc, id asc rows between 6 preceding and current row) last7_avg_amount_filled,
        sum(amount_filled) over (partition by merchant order by amount asc, id asc rows between 6 preceding and current row) last7_sum_amount_filled
    from filtered_transactions
)
-- Ranking and gap features
select
    *,
    lead(last7_sum_amount_filled) over (partition by merchant order by amount_filled asc, id asc) next_last7_sum_amount_filled,
    lead(amount) over (partition by merchant order by amount_filled asc, id asc) next_amount,
    row_number() over (partition by merchant order by amount_filled asc, id asc) event_seq
from with_windows
where last7_count_amount_filled >= 13
