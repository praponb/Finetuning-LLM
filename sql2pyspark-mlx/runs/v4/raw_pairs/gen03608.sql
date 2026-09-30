with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        ntile(5) over (partition by status, plan order by monthly_fee desc, id) as monthly_fee_bucket,
        avg(monthly_fee) over (partition by plan order by monthly_fee, id rows between 3 preceding and 1 preceding) as monthly_fee_avg_3_prior,
        ntile(4) over (order by monthly_fee, id) as monthly_fee_bucket_2
    from subscriptions
),
with_windows_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by monthly_fee_bucket_2 nulls first) as monthly_fee_bucket_2_rank,
        lag(monthly_fee_bucket_2, 1) over (order by monthly_fee_avg_3_prior, id) as prev_monthly_fee_bucket_2
    from with_windows
),
valid as (
    -- Basic cleansing and defaults
    select *, ceil(prev_monthly_fee_bucket_2) as prev_monthly_fee_bucket_2_whole
    from with_windows_subscriptions
    where end_date > to_date('2024-04-10')
      and customer_id is not null
),
with_windows_subscriptions_2 as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_bucket,
        monthly_fee_avg_3_prior,
        monthly_fee_bucket_2,
        monthly_fee_bucket_2_rank,
        prev_monthly_fee_bucket_2,
        prev_monthly_fee_bucket_2_whole,
        sum(prev_monthly_fee_bucket_2) over (order by monthly_fee, id rows between unbounded preceding and current row) as running_prev_monthly_fee_bucket_2,
        ntile(3) over (partition by plan order by monthly_fee_bucket_2_rank desc, id) as monthly_fee_bucket_2_rank_bucket,
        avg(monthly_fee_avg_3_prior) over (partition by plan order by monthly_fee_avg_3_prior, id rows between 6 preceding and current row) as monthly_fee_avg_3_prior_avg_6,
        row_number() over (order by monthly_fee_bucket_2 desc, id) as event_seq
    from valid
),
joined as (
    -- Attach customers attributes
    select s.*, c.segment
    from with_windows_subscriptions_2 s
    left join customers c
        on s.customer_id = c.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by plan order by prev_monthly_fee_bucket_2 desc) as prev_monthly_fee_bucket_2_dense_rank,
        first_value(prev_monthly_fee_bucket_2) ignore nulls over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) as first_known_prev_monthly_fee_bucket_2,
        row_number() over (partition by customer_id order by monthly_fee_avg_3_prior desc, id) as seq_no
    from joined
)
-- Aggregate with conditional metrics
select
    plan,
    sum(monthly_fee_avg_3_prior_avg_6) as total_monthly_fee_avg_3_prior_avg_6,
    count_if(status = 'EXPIRED') as expired_events,
    round(sum(prev_monthly_fee_bucket_2_dense_rank) / nullif(count(case when plan = 'basic' then 1 end), 0), 2) as prev_monthly_fee_bucket_2_dense_rank_per_basic,
    count(*) filter (where status = 'EXPIRED') as expired_rows
from scored
group by plan
order by prev_monthly_fee_bucket_2_dense_rank_per_basic;
