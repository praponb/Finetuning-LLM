with enriched as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over w_hist monthly_fee_cume_dist,
        cume_dist() over (partition by plan order by monthly_fee desc nulls first) monthly_fee_cume_dist_2,
        ntile(10) over w_grp monthly_fee_bucket,
        cume_dist() over (partition by plan order by monthly_fee nulls last) monthly_fee_cume_dist_3
    from subscriptions
    window w_hist as (partition by status order by monthly_fee desc nulls last), w_grp as (partition by status order by monthly_fee desc, id)
),
enriched_subscriptions as (
    -- Attach customers attributes
    select s.*, b.age
    from enriched s
    left join customers b
        on s.customer_id = b.id
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        monthly_fee_cume_dist_2,
        age,
        substring(plan, 1, 4) plan_prefix
    from enriched_subscriptions
    where monthly_fee_cume_dist_2 > 0.43
),
scored as (
    -- Ranking and gap features
    select
        *,
        percent_rank() over w_running age_percent_rank,
        sum(monthly_fee) over w_running_2 customer_id_sum_monthly_fee
    from cleaned
    window w_running as (order by age desc nulls last), w_running_2 as (partition by customer_id)
)
select
    id,
    start_date,
    end_date,
    monthly_fee,
    monthly_fee_cume_dist_2,
    age,
    customer_id_sum_monthly_fee
from scored
order by age, customer_id_sum_monthly_fee;
