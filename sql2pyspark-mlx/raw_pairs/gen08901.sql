with enriched as (
    -- Attach customers attributes
    select
        e.*,
        b.email,
        b.segment,
        b.signup_date
    from subscriptions e
    inner join customers b
        on e.customer_id = b.id
    where b.age < 54
),
metrics as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        plan,
        end_date,
        monthly_fee,
        segment,
        signup_date,
        avg(monthly_fee) over w_ordered as running_avg_monthly_fee,
        max(monthly_fee) over w_ordered as running_max_monthly_fee,
        sum(monthly_fee) over w_ordered as running_sum_monthly_fee,
        ntile(3) over w_part as monthly_fee_bucket
    from enriched
    window w_ordered as (partition by segment, plan order by monthly_fee asc, id asc rows between unbounded preceding and current row), w_part as (partition by status, segment order by monthly_fee desc, id asc)
),
enriched_subscriptions as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        segment,
        signup_date,
        running_avg_monthly_fee,
        monthly_fee_bucket,
        cume_dist() over (partition by customer_id, segment order by running_sum_monthly_fee asc) as running_sum_monthly_fee_cume_dist,
        avg(monthly_fee_bucket) over (partition by plan order by monthly_fee_bucket asc, id asc rows between 2 preceding and current row) as monthly_fee_bucket_avg_2,
        round(running_sum_monthly_fee / sum(running_sum_monthly_fee) over (partition by month(signup_date), customer_id), 4) as running_sum_monthly_fee_share,
        row_number() over (partition by segment order by running_sum_monthly_fee desc, id asc) as row_seq,
        lag(monthly_fee, 1) over (partition by segment order by end_date asc, id asc) as prev_monthly_fee
    from metrics
),
with_dims as (
    -- Attach customers attributes
    select
        e.*,
        p.email,
        p.signup_date as customer_signup_date
    from enriched_subscriptions e
    left join customers p
        on e.customer_id = p.id
)
-- Grouped metrics
select
    customer_id,
    case when monthly_fee_bucket >= 7 then 'PREMIUM' else 'STANDARD' end as monthly_fee_bucket_tier,
    approx_percentile(row_seq, 0.95) as row_seq_p95,
    sum(case when segment = 'Consumer' then running_sum_monthly_fee_share else 0 end) as consumer_running_sum_monthly_fee_share,
    round(sum(monthly_fee_bucket) / nullif(count(case when segment = 'Corporate' then 1 end), 0), 2) as monthly_fee_bucket_per_corporate
from with_dims
group by rollup (customer_id, case when monthly_fee_bucket >= 7 then 'PREMIUM' else 'STANDARD' end)
order by consumer_running_sum_monthly_fee_share desc nulls first;
