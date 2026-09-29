with valid as (
    -- Keep recent, valid rows
    select *, concat_ws('-', merchant, txn_type) as merchant_txn_type_key
    from transactions
    where merchant is not null
      and txn_type not in ('DEBIT', 'REFUND', 'FEE')
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        sum(amount) over (partition by merchant order by amount, id rows between unbounded preceding and current row) as running_amount,
        max(amount) over (partition by account_id order by cast(txn_time as bigint) range between 2592000 preceding and current row) as amount_max_30d
    from valid
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        merchant_txn_type_key,
        amount_max_30d,
        least(amount_max_30d, amount) as least_amount_max_30d_amount,
        round(amount, 2) as amount_rounded,
        coalesce(merchant, 'OTHER') as resolved_merchant
    from scored
    where not is_flagged
),
latest as (
    -- QUALIFY-style window filter
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant_txn_type_key,
        amount_rounded,
        resolved_merchant
    from (
        select *, rank() over (partition by txn_type order by least_amount_max_30d_amount desc) as rn
        from base
    ) x
    where rn = 1
),
scored_transactions as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_hist as amount_rounded_cume_dist,
        avg(amount) over (partition by year(txn_time), txn_type order by amount_rounded, id rows between 6 preceding and 1 preceding) as amount_avg_6_prior
    from latest
    where amount > 1500
    window w_hist as (partition by account_id order by amount_rounded nulls first)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        amount,
        merchant_txn_type_key,
        amount_rounded,
        resolved_merchant,
        ntile(4) over (partition by to_date(txn_time), account_id order by amount_avg_6_prior, id) as amount_avg_6_prior_bucket,
        count(amount_rounded_cume_dist) over (partition by account_id order by cast(txn_time as bigint) range between 7776000 preceding and current row) as amount_rounded_cume_dist_count_90d
    from scored_transactions
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when amount_avg_6_prior_bucket >= 8 then 'High' when amount_avg_6_prior_bucket >= 3 then 'Medium' else 'Low' end as amount_avg_6_prior_bucket_tier,
        case when amount >= 3800 then 'High' when amount >= 1300 then 'Medium' else 'Low' end as amount_tier
    from metrics
    where amount_rounded >= 40
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        amount,
        amount_rounded
    from cleaned
) src
unpivot (
    val for kpi in (amount, amount_rounded as AMOUNT_ROUNDED_VAL)
);
