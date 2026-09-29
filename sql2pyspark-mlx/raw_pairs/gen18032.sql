with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        start_date,
        monthly_fee,
        status,
        concat_ws('-', status, plan) as status_plan_key
    from subscriptions
    where start_date > to_date('2023-03-15')
      and customer_id is null
),
metrics as (
    -- Ranking and gap features
    select
        *,
        row_number() over (partition by plan order by monthly_fee, id) as event_seq,
        avg(monthly_fee) over (partition by year(start_date), plan order by monthly_fee, id rows between 2 preceding and 1 preceding) as monthly_fee_avg_2_prior,
        round(monthly_fee / sum(monthly_fee) over (partition by year(start_date), status), 4) as monthly_fee_share
    from filtered
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        plan,
        start_date,
        status,
        status_plan_key,
        monthly_fee_avg_2_prior,
        monthly_fee_share,
        monthly_fee + monthly_fee_share as monthly_fee_plus_monthly_fee_share
    from metrics
    where start_date > date '2023-03-26'
      and plan != 'basic'
),
metrics_subscriptions as (
    -- Ranking and gap features
    select
        id,
        plan,
        start_date,
        status,
        status_plan_key,
        monthly_fee_avg_2_prior,
        monthly_fee_plus_monthly_fee_share,
        dense_rank() over (partition by plan, status order by monthly_fee_plus_monthly_fee_share nulls first) as monthly_fee_plus_monthly_fee_share_dense_rank,
        percent_rank() over (partition by status order by monthly_fee_avg_2_prior desc nulls last) as monthly_fee_avg_2_prior_percent_rank
    from valid
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by monthly_fee_plus_monthly_fee_share nulls first, id) as dedup_rank
    from metrics_subscriptions
    qualify dedup_rank = 1
)
-- Running and moving aggregates
select
    id,
    plan,
    start_date,
    status,
    status_plan_key,
    monthly_fee_plus_monthly_fee_share,
    lead(monthly_fee_avg_2_prior_percent_rank) over (partition by plan, status order by dedup_rank, id) as next_monthly_fee_avg_2_prior_percent_rank,
    lag(monthly_fee_avg_2_prior_percent_rank) over (partition by plan, status order by dedup_rank, id) as prev_monthly_fee_avg_2_prior_percent_rank,
    lag(monthly_fee_plus_monthly_fee_share_dense_rank) over (partition by plan, status order by dedup_rank, id) as prev_monthly_fee_plus_monthly_fee_share_dense_rank,
    row_number() over (partition by plan, status order by dedup_rank, id) as seq_no
from ranked
