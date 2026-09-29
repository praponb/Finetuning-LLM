with base as (
    -- Keep recent, valid rows
    select
        id,
        plan,
        start_date,
        end_date,
        status,
        date_add(start_date, 90) start_date_plus_90d
    from subscriptions
    where customer_id is not null
),
scored as (
    -- Window metrics over several different partitions
    select *, row_number() over (partition by plan order by end_date, id) row_seq
    from base
),
scored_subscriptions as (
    -- Running and moving aggregates
    select
        id,
        plan,
        row_number() over (partition by status order by start_date_plus_90d desc, id) seq_no,
        row_number() over (partition by plan order by end_date desc, id) seq_no_2,
        avg(row_seq) over (partition by status order by start_date, id rows between 5 preceding and 1 preceding) row_seq_avg_5_prior,
        sum(row_seq) over (partition by year(start_date_plus_90d), plan) start_date_plus_90d_year_sum_row_seq
    from scored
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(seq_no_2) over w_running next_seq_no_2,
        lag(seq_no) over w_running prev_seq_no,
        lead(seq_no) over w_running next_seq_no,
        row_number() over w_running seq_no_3
    from scored_subscriptions
    window w_running as (partition by plan order by seq_no, id)
)
select
    id,
    plan,
    seq_no,
    seq_no_2,
    row_seq_avg_5_prior,
    start_date_plus_90d_year_sum_row_seq,
    prev_seq_no,
    next_seq_no
from metrics
order by seq_no_2 desc nulls last;
