with filtered as (
    select
        *,
        cast(is_flagged as int) as is_flagged_flag,
        weekofyear(txn_time) as txn_time_weekofyear
    from transactions
    where merchant is not null
),
valid as (
    select *, case when amount >= 4500 then 'High' when amount >= 1000 then 'Medium' else 'Low' end as amount_band
    from filtered
    where txn_time >= to_timestamp('2023-04-30 16:00:00')
),
cleaned as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_weekofyear,
        amount_band,
        substring(merchant, 1, 3) as merchant_prefix,
        coalesce(merchant, 'NONE') as merchant_clean
    from valid
    where merchant is not null
      and txn_time >= timestamp '2023-10-22 08:00:00'
),
metrics as (
    select
        id,
        txn_type,
        txn_time,
        merchant_prefix,
        max(amount) over (partition by txn_type order by cast(txn_time as bigint) range between 2592000 preceding and current row) as amount_max_30d,
        count(txn_time_weekofyear) over (partition by account_id order by cast(txn_time as bigint) range between 604800 preceding and current row) as txn_time_weekofyear_count_7d
    from cleaned
),
cleaned_transactions as (
    select
        id,
        txn_type,
        amount_max_30d,
        txn_time_weekofyear_count_7d,
        date_format(txn_time, 'yyyy-MM-dd') as txn_time_label
    from metrics
    where txn_time >= timestamp '2023-10-13 00:00:00'
      and txn_time_weekofyear_count_7d > 10
),
enriched as (
    select
        id,
        txn_type,
        amount_max_30d,
        txn_time_weekofyear_count_7d,
        txn_time_label,
        count(txn_time_weekofyear_count_7d) over (partition by txn_type) as txn_type_count_txn_time_weekofyear_count_7d,
        ntile(3) over (partition by txn_type order by amount_max_30d, id) as amount_max_30d_bucket,
        lead(txn_time_weekofyear_count_7d, 1, 0) over (partition by txn_type order by txn_time_weekofyear_count_7d, id) as next_txn_time_weekofyear_count_7d
    from cleaned_transactions
    where amount_max_30d <> 2360
),
metrics_transactions as (
    select
        id,
        txn_type,
        amount_max_30d,
        amount_max_30d_bucket,
        sum(txn_type_count_txn_time_weekofyear_count_7d) over (partition by txn_type order by next_txn_time_weekofyear_count_7d, id rows between unbounded preceding and current row) as cumulative_txn_type_count_txn_time_weekofyear_count_7d,
        sum(txn_time_weekofyear_count_7d) over (order by txn_time_weekofyear_count_7d, id rows between unbounded preceding and current row) as cumulative_txn_time_weekofyear_count_7d,
        avg(txn_type_count_txn_time_weekofyear_count_7d) over w_running as txn_type_count_txn_time_weekofyear_count_7d_avg_5,
        last_value(txn_type_count_txn_time_weekofyear_count_7d, true) over (partition by txn_type order by txn_type_count_txn_time_weekofyear_count_7d, id rows between unbounded preceding and current row) as last_known_txn_type_count_txn_time_weekofyear_count_7d,
        row_number() over (order by amount_max_30d, id) as seq_no
    from enriched
    window w_running as (partition by txn_type order by txn_type_count_txn_time_weekofyear_count_7d, id rows between 5 preceding and current row)
)
select
    id,
    txn_type,
    amount_max_30d_bucket,
    cumulative_txn_type_count_txn_time_weekofyear_count_7d,
    cumulative_txn_time_weekofyear_count_7d,
    last_known_txn_type_count_txn_time_weekofyear_count_7d,
    seq_no,
    case when txn_type rlike '^(REFUND|FEE)$' then 'GROUP_B' when txn_type rlike '^(DEBIT|CREDIT)$' then 'PRIORITY' else 'CORE' end as txn_type_zone
from metrics_transactions
order by cumulative_txn_type_count_txn_time_weekofyear_count_7d, seq_no
