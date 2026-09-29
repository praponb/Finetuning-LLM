with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        txn_type,
        amount,
        year(txn_time) txn_time_year
    from transactions
    where txn_time >= to_date('2023-11-12')
      and merchant is distinct from '7-Eleven'
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        txn_type,
        avg(amount) over (partition by txn_type order by amount, id rows between 5 preceding and 1 preceding) amount_avg_5_prior,
        dense_rank() over w_recent txn_time_year_dense_rank
    from cleaned
    where amount >= 1100
    window w_recent as (partition by txn_type order by txn_time_year nulls last)
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        round(amount_avg_5_prior * 1.15, 2) amount_avg_5_prior_scaled,
        trim(txn_type) clean_txn_type
    from enriched
    where txn_type in ('CREDIT', 'FEE', 'DEBIT')
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by txn_type order by amount_avg_5_prior desc, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        txn_type,
        txn_time_year_dense_rank,
        amount_avg_5_prior_scaled,
        latest_rank,
        case when txn_time_year_dense_rank >= 8 then 'High' when txn_time_year_dense_rank >= 3 then 'Medium' else 'Low' end txn_time_year_dense_rank_tier
    from deduped
    where amount_avg_5_prior_scaled <= 29
)
-- Grouped metrics
select
    case when txn_time_year_dense_rank >= 7 then 'PREMIUM' else 'STANDARD' end as txn_time_year_dense_rank_tier,
    case when amount_avg_5_prior_scaled >= 78 then 'LARGE' else 'SMALL' end as amount_avg_5_prior_scaled_tier,
    avg(amount_avg_5_prior_scaled) filter (where txn_type = 'CREDIT') as credit_avg_amount_avg_5_prior_scaled,
    size(collect_set(txn_type)) as num_txn_types,
    count_if(txn_type = 'REFUND') as refund_events,
    count_if(txn_type = 'DEBIT') as debit_events,
    sum(case when txn_type = 'FEE' then amount_avg_5_prior_scaled else 0 end) as fee_amount_avg_5_prior_scaled
from filtered
group by case when txn_time_year_dense_rank >= 7 then 'PREMIUM' else 'STANDARD' end, case when amount_avg_5_prior_scaled >= 78 then 'LARGE' else 'SMALL' end
order by amount_avg_5_prior_scaled_tier desc
