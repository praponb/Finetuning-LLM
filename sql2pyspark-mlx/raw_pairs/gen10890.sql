with ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by txn_type order by amount desc nulls last, id) as row_num
    from transactions
    qualify row_num = 1
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        row_number() over (partition by month(txn_time), account_id order by txn_time desc, id) as row_seq,
        round(amount / sum(amount) over (partition by txn_type), 4) as amount_share
    from ranked
),
metrics_transactions as (
    -- Window metrics over several different partitions
    select
        id,
        txn_type,
        merchant,
        is_flagged,
        row_seq,
        amount_share,
        sum(amount_share) over w_part as running_sum_amount_share,
        min(amount_share) over w_part as running_min_amount_share,
        max(amount_share) over w_part as running_max_amount_share
    from metrics
    where amount_share <= 0.3
    window w_part as (partition by merchant order by amount_share, id rows between unbounded preceding and current row)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by txn_type order by running_min_amount_share, id) as rn
        from metrics_transactions
    ) t
    where rn = 1
),
stats as (
    -- Multi-level aggregation
    select
        txn_type,
        case when row_seq >= 6 then 'LARGE' else 'SMALL' end as row_seq_tier,
        count(*) filter (where txn_type = 'FEE') as fee_rows,
        round(avg(running_min_amount_share), 2) as avg_running_min_amount_share,
        percentile_approx(running_min_amount_share, array(0.25, 0.5, 0.75)) as running_min_amount_share_quartiles,
        grouping(txn_type) as grouping_id
    from latest
    group by grouping sets ((txn_type, case when row_seq >= 6 then 'LARGE' else 'SMALL' end), (txn_type))
)
select
    txn_type,
    row_seq_tier,
    fee_rows,
    avg_running_min_amount_share,
    running_min_amount_share_quartiles,
    grouping_id
from stats
order by grouping_id desc
