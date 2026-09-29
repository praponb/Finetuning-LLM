with base as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        trim(merchant) as clean_merchant,
        lower(merchant) as merchant_lower,
        coalesce(merchant, 'DIRECT') as merchant_filled
    from transactions
    where merchant is not null
      and merchant is distinct from '7-Eleven'
),
scored as (
    select
        id,
        txn_type,
        is_flagged,
        row_number() over (partition by merchant order by txn_time desc, id) as event_seq,
        percent_rank() over (partition by month(txn_time), merchant order by amount) as amount_percent_rank
    from base
),
filtered as (
    select
        *,
        least(event_seq, amount_percent_rank) as least_event_seq_amount_percent_rank,
        if(is_flagged, 'Yes', 'No') as is_flagged_label
    from scored
    where amount_percent_rank between 0.53 and 0.6
),
cleaned as (
    select
        id,
        txn_type,
        is_flagged,
        event_seq,
        amount_percent_rank,
        least_event_seq_amount_percent_rank,
        is_flagged_label,
        least_event_seq_amount_percent_rank - amount_percent_rank as least_event_seq_amount_percent_rank_minus_amount_percent_rank
    from filtered
    where amount_percent_rank <= 0.4
)
select
    txn_type,
    count(case when txn_type = 'DEBIT' then 1 end) as debit_count,
    sort_array(collect_set(txn_type)) as txn_type_list,
    count(case when txn_type = 'FEE' then 1 end) as fee_count,
    approx_percentile(least_event_seq_amount_percent_rank_minus_amount_percent_rank, array(0.25, 0.5, 0.75)) as least_event_seq_amount_percent_rank_minus_amount_percent_rank_quartiles,
    round(sum(least_event_seq_amount_percent_rank_minus_amount_percent_rank) / nullif(count(case when txn_type = 'DEBIT' then 1 end), 0), 2) as least_event_seq_amount_percent_rank_minus_amount_percent_rank_per_debit,
    sum(amount_percent_rank) filter (where txn_type = 'DEBIT') as debit_total_amount_percent_rank
from cleaned
group by 1
having debit_count >= 1;
