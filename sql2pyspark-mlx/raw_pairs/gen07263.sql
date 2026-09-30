with cleaned as (
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        concat_ws(',', plan, status) as plan_status_key
    from subscriptions
    where customer_id is not null
),
ranked as (
    select *, row_number() over (partition by status, plan order by start_date desc, id asc) as latest_rank
    from cleaned
    qualify latest_rank = 1
),
cleaned_subscriptions as (
    select
        *,
        round(latest_rank, 2) as latest_rank_rounded,
        length(plan) as plan_length
    from ranked
    where end_date > date '2023-10-05'
),
with_windows as (
    select
        id,
        start_date,
        end_date,
        status,
        latest_rank_rounded,
        plan_length,
        row_number() over (partition by plan order by end_date asc, id asc) as row_seq,
        ntile(4) over (order by plan_length desc, id asc) as plan_length_bucket,
        max(plan_length) over (partition by plan, customer_id) as plan_max_plan_length
    from cleaned_subscriptions
)
select
    id,
    start_date,
    end_date,
    status,
    latest_rank_rounded,
    row_seq,
    plan_length_bucket,
    plan_max_plan_length,
    ntile(3) over (partition by year(start_date), status order by plan_length asc, id asc) as plan_length_bucket_2,
    dense_rank() over (partition by status order by plan_length desc nulls last) as plan_length_dense_rank
from with_windows;
