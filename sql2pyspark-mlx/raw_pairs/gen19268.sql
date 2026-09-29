with filtered as (
    select
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        concat(merchant, ' ', txn_type) as merchant_txn_type
    from transactions
    where merchant is not null
      and is_flagged = false
),
enriched as (
    select *, last_value(merchant, true) over (partition by merchant, txn_type order by txn_time, id rows between unbounded preceding and current row) as last_known_merchant
    from filtered
    where is_flagged
),
latest as (
    select
        id,
        txn_type,
        merchant,
        is_flagged,
        merchant_txn_type,
        last_known_merchant
    from enriched
    qualify row_number() over (partition by merchant order by txn_time desc nulls last, id) = 1
),
valid as (
    select *, coalesce(merchant, 'Unknown') as merchant_filled
    from latest
    where merchant is not null
),
cleaned as (
    select
        id,
        merchant,
        is_flagged,
        merchant_txn_type,
        merchant_filled,
        upper(merchant) as merchant_upper,
        coalesce(merchant, 'DIRECT') as resolved_merchant
    from valid
    where merchant is not null
      and txn_type in ('REFUND', 'FEE')
),
valid_transactions as (
    select
        *,
        lower(merchant) as merchant_lower,
        cast(is_flagged as int) as is_flagged_flag
    from cleaned
    where merchant <> 'Amazon'
),
metrics as (
    select
        *,
        lag(is_flagged_flag) over w_running as prev_is_flagged_flag,
        lead(is_flagged_flag) over w_running as next_is_flagged_flag,
        row_number() over w_running as seq_no
    from valid_transactions
    window w_running as (partition by merchant order by is_flagged_flag, id)
)
select
    id,
    is_flagged,
    merchant_txn_type,
    resolved_merchant,
    is_flagged_flag,
    prev_is_flagged_flag,
    next_is_flagged_flag,
    seq_no
from metrics
order by is_flagged_flag desc nulls last, prev_is_flagged_flag
