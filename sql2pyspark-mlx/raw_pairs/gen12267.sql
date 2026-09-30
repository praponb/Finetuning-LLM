with metrics as (
    -- Running and moving aggregates
    select
        id,
        merchant,
        is_flagged,
        dense_rank() over (order by amount desc nulls last) as amount_dense_rank,
        lag(amount, 1) over (partition by merchant order by txn_time, id) as prev_amount,
        lead(amount, 2, 0) over w_running as next_amount
    from transactions
    window w_running as (partition by merchant, txn_type order by txn_time, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by merchant order by next_amount desc nulls first, id) as dedup_rank
        from metrics
    ) t
    where dedup_rank = 1
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by merchant order by amount_dense_rank nulls last, id) as rank_in_group
        from latest
    ) ranked
    where rank_in_group = 1
),
ranked_transactions as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by merchant order by rank_in_group desc, id) as latest_rank
    from ranked
    qualify latest_rank = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(10) over w_hist as prev_amount_bucket,
        avg(next_amount) over (partition by merchant order by next_amount, id rows between 5 preceding and 1 preceding) as next_amount_avg_5_prior,
        avg(amount_dense_rank) over w_running as amount_dense_rank_avg_5,
        first_value(merchant, true) over (partition by merchant order by next_amount, id rows between unbounded preceding and current row) as first_known_merchant,
        rank() over w_hist_2 as prev_amount_rank
    from ranked_transactions
    window w_hist as (order by prev_amount desc, id), w_running as (partition by merchant order by dedup_rank, id rows between 5 preceding and current row), w_hist_2 as (order by prev_amount desc)
)
select
    id,
    merchant,
    is_flagged,
    amount_dense_rank,
    prev_amount,
    next_amount,
    rank_in_group,
    latest_rank,
    prev_amount_bucket
from enriched
