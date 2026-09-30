with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        floor(amount) as amount_whole,
        lower(merchant) as merchant_lower
    from transactions
    where txn_time >= date '2023-06-03'
      and merchant is not null
),
enriched as (
    -- Ranking and gap features
    select
        id,
        amount,
        txn_time,
        amount_whole,
        merchant_lower,
        lag(amount) over w_recent as prev_amount,
        lead(amount_whole) over w_recent as next_amount_whole,
        lag(amount_whole) over w_recent as prev_amount_whole,
        row_number() over w_recent as seq_no,
        max(amount) over (partition by merchant) as merchant_max_amount
    from valid
    where hour(txn_time) between 8 and 14
    window w_recent as (partition by merchant order by amount asc, id asc)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        txn_time,
        merchant_lower,
        next_amount_whole,
        prev_amount_whole,
        seq_no,
        merchant_max_amount,
        ntile(4) over (order by amount_whole asc, id asc) as amount_whole_bucket,
        rank() over w_hist as next_amount_whole_rank,
        first_value(prev_amount) ignore nulls over (order by next_amount_whole asc, id asc rows between unbounded preceding and current row) as first_known_prev_amount,
        last_value(merchant_max_amount, true) over w_hist_2 as last_known_merchant_max_amount,
        sum(amount) over w_hist_2 as running_amount
    from enriched
    window w_hist as (order by next_amount_whole asc nulls first), w_hist_2 as (order by seq_no asc, id asc rows between unbounded preceding and current row)
),
enriched_transactions as (
    -- Running and moving aggregates
    select
        id,
        merchant_lower,
        next_amount_whole,
        prev_amount_whole,
        seq_no,
        merchant_max_amount,
        last_known_merchant_max_amount,
        running_amount,
        row_number() over (partition by to_date(txn_time) order by txn_time desc, id asc) as seq_no_2,
        avg(merchant_max_amount) over (partition by to_date(txn_time) order by prev_amount_whole asc, id asc rows between 2 preceding and current row) as merchant_max_amount_avg_2,
        row_number() over (partition by year(txn_time) order by seq_no asc, id asc) as event_seq
    from metrics
)
-- Grouped metrics
select
    seq_no,
    count(*) as cnt,
    round(avg(prev_amount_whole), 2) as avg_prev_amount_whole,
    min(seq_no) as min_seq_no,
    approx_percentile(next_amount_whole, 0.95) as next_amount_whole_p95
from enriched_transactions
group by seq_no
order by min_seq_no asc
