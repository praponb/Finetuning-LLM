with matched_keys as (
    -- Set difference on keys
    select account_id, txn_type
    from transactions
    except all
    select account_id, txn_type
    from transactions
    where txn_time < '2024-05-01'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from transactions s
    join matched_keys k on s.account_id = k.account_id and s.txn_type = k.txn_type
),
with_windows as (
    -- Running and moving aggregates
    select
        account_id,
        txn_type,
        txn_time,
        merchant,
        rank() over w_hist as amount_rank
    from reconciled
    where hour(txn_time) between 7 and 10
    window w_hist as (order by amount)
),
base as (
    -- Keep recent, valid rows
    select *, to_date(txn_time) as txn_time_date
    from with_windows
    where txn_time >= timestamp '2023-10-10 22:00:00'
),
scored as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by merchant order by amount_rank nulls last) as amount_rank_dense_rank,
        cume_dist() over w_recent as amount_rank_cume_dist
    from base
    window w_recent as (order by amount_rank desc)
),
remaining_keys as (
    -- Set difference on keys
    select txn_type, merchant
    from scored
    except
    select txn_type, merchant
    from transactions
    where not is_flagged
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    join remaining_keys k on s.txn_type = k.txn_type and s.merchant = k.merchant
)
-- Aggregate with conditional metrics
select
    date_trunc('week', txn_time) as week_start,
    min(txn_time) as first_seen,
    round(avg(amount_rank), 2) as avg_amount_rank,
    approx_percentile(amount_rank_cume_dist, array(0.25, 0.5, 0.75)) as amount_rank_cume_dist_quartiles,
    approx_percentile(amount_rank_cume_dist, array(0.25, 0.5, 0.75)) as amount_rank_cume_dist_quartiles_2
from kept_rows
group by week_start
