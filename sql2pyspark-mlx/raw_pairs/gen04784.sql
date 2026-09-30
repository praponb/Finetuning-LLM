with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        lower(plan) as plan_lower
    from subscriptions
    where end_date > date '2023-06-07'
      and customer_id is not null
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        customer_id,
        start_date,
        end_date,
        plan_lower
    from cleaned
    qualify row_number() over (partition by customer_id order by end_date, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        start_date,
        end_date,
        row_number() over (partition by customer_id order by end_date, id) as seq_no,
        row_number() over (partition by customer_id order by end_date desc, id) as seq_no_2
    from deduped
    where customer_id is not null
),
enriched_subscriptions as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.start_date,
        b.signup_date
    from enriched src
    join customers b
        on src.customer_id = b.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, date_sub(start_date, 40) as start_date_minus_40d
    from enriched_subscriptions
    where start_date > date '2023-05-29'
      and customer_id is not null
),
scored as (
    -- Ranking and gap features
    select
        id,
        start_date,
        signup_date,
        start_date_minus_40d,
        row_number() over (order by start_date, id) as row_seq,
        row_number() over (partition by customer_id order by signup_date, id) as event_seq,
        row_number() over (partition by customer_id order by signup_date, id) as event_seq_2,
        row_number() over (partition by customer_id order by start_date desc, id) as seq_no,
        row_number() over (partition by customer_id order by signup_date, id) as event_seq_3
    from base
),
metrics as (
    -- Ranking and gap features
    select
        *,
        first_value(event_seq, true) over w_grp as first_known_event_seq,
        ntile(5) over w_grp_2 as seq_no_bucket,
        round(event_seq / sum(event_seq) over (partition by year(start_date)), 4) as event_seq_share,
        ntile(4) over (order by event_seq_3 desc, id) as event_seq_3_bucket,
        last_value(event_seq, true) over (order by row_seq, id rows between unbounded preceding and current row) as last_known_event_seq
    from scored
    window w_grp as (order by seq_no, id rows between unbounded preceding and current row), w_grp_2 as (order by seq_no, id)
)
-- Aggregate with conditional metrics
select
    date_trunc('month', start_date_minus_40d) as activity_month,
    count(*) as event_count,
    approx_percentile(event_seq_2, 0.9) as event_seq_2_p90,
    count(*) as event_count_2,
    percentile_approx(seq_no_bucket, 0.5) as seq_no_bucket_p50,
    count(distinct id) as distinct_ids
from metrics
group by 1
order by activity_month desc nulls last;
