with with_windows as (
    select
        id,
        customer_id,
        plan,
        end_date,
        sum(monthly_fee) over (order by end_date, id rows between unbounded preceding and current row) as running_monthly_fee,
        percent_rank() over (partition by plan order by monthly_fee) as monthly_fee_percent_rank,
        sum(monthly_fee) over (partition by status order by start_date, id rows between unbounded preceding and current row) as running_monthly_fee_2,
        lag(monthly_fee, 1, 0) over (partition by date_trunc('MONTH', start_date), customer_id order by end_date, id) as prev_monthly_fee
    from subscriptions
),
with_dims as (
    select
        s.*,
        d.age,
        d.country,
        d.email
    from with_windows s
    inner join customers d
        on s.customer_id = d.id
),
with_windows_subscriptions as (
    select
        *,
        ntile(3) over (partition by plan order by running_monthly_fee desc, id) as running_monthly_fee_bucket,
        row_number() over (order by running_monthly_fee_2 desc, id) as event_seq,
        count(running_monthly_fee_2) over (partition by plan) as plan_count_running_monthly_fee_2
    from with_dims
),
latest as (
    select
        id,
        plan,
        running_monthly_fee,
        monthly_fee_percent_rank,
        running_monthly_fee_2,
        prev_monthly_fee,
        email
    from (
        select *, row_number() over (partition by customer_id, plan order by running_monthly_fee_2 nulls last, id) as rn
        from with_windows_subscriptions
    ) ranked
    where rn = 1
),
deduped as (
    select *
    from (
        select *, row_number() over (partition by plan order by running_monthly_fee desc nulls last, id) as row_num
        from latest
    ) t
    where row_num = 1
)
select
    case when prev_monthly_fee >= 197 then 'PREMIUM' else 'STANDARD' end as prev_monthly_fee_tier,
    round(avg(running_monthly_fee_2), 2) as avg_running_monthly_fee_2,
    sum(case when plan = 'premium' then monthly_fee_percent_rank else 0 end) as premium_monthly_fee_percent_rank
from deduped
group by case when prev_monthly_fee >= 197 then 'PREMIUM' else 'STANDARD' end
having avg_running_monthly_fee_2 > 0;
