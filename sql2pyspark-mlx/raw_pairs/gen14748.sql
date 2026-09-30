with filtered as (
    select
        *,
        round(monthly_fee * 1.1, 2) as adjusted_monthly_fee,
        initcap(trim(plan)) as plan_title
    from subscriptions
    where end_date >= to_date('2023-05-23')
      and customer_id is not null
),
scored as (
    select
        id,
        plan,
        status,
        adjusted_monthly_fee,
        dense_rank() over (partition by plan, customer_id order by adjusted_monthly_fee desc) as adjusted_monthly_fee_dense_rank,
        stddev_samp(adjusted_monthly_fee) over (partition by customer_id) as customer_id_stddev_samp_adjusted_monthly_fee,
        first_value(adjusted_monthly_fee) ignore nulls over (partition by customer_id order by end_date asc, id asc rows between unbounded preceding and current row) as first_known_adjusted_monthly_fee,
        lag(monthly_fee, 1) over (partition by status order by monthly_fee asc, id asc) as prev_monthly_fee,
        lag(monthly_fee, 1) over (partition by plan order by end_date asc, id asc) as prev_monthly_fee_2
    from filtered
),
enriched as (
    select
        *,
        ntile(5) over (partition by status order by first_known_adjusted_monthly_fee asc, id asc) as first_known_adjusted_monthly_fee_bucket,
        lag(prev_monthly_fee_2, 1, 0) over (partition by plan order by prev_monthly_fee_2 asc, id asc) as prev_prev_monthly_fee_2,
        sum(first_known_adjusted_monthly_fee) over (partition by status order by prev_monthly_fee asc, id asc rows between unbounded preceding and current row) as cumulative_first_known_adjusted_monthly_fee
    from scored
),
deduped as (
    select
        id,
        plan,
        status,
        adjusted_monthly_fee,
        adjusted_monthly_fee_dense_rank,
        customer_id_stddev_samp_adjusted_monthly_fee,
        first_known_adjusted_monthly_fee,
        prev_monthly_fee,
        prev_prev_monthly_fee_2
    from enriched
    qualify rank() over (partition by plan order by first_known_adjusted_monthly_fee desc nulls first) = 1
)
select
    status,
    round(sum(prev_prev_monthly_fee_2) / nullif(count(case when plan = 'basic' then 1 end), 0), 2) as prev_prev_monthly_fee_2_per_basic,
    count(*) as row_count,
    grouping(status) as grouping_level
from deduped
group by cube (status)
order by prev_prev_monthly_fee_2_per_basic desc;
