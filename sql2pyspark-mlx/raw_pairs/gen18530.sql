with filtered as (
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        case when monthly_fee >= 184 then 'High' when monthly_fee >= 96 then 'Medium' else 'Low' end as monthly_fee_tier,
        round(datediff(end_date, start_date) / 30, 1) as months_start_date_to_end_date
    from subscriptions
    where customer_id is not null
      and start_date >= date '2023-04-19'
),
filtered_subscriptions as (
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        monthly_fee_tier,
        concat_ws(',', plan, status) as plan_status_key
    from filtered
    where end_date > date '2023-08-06'
),
metrics as (
    select
        *,
        avg(monthly_fee) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) as running_avg_monthly_fee,
        max(monthly_fee) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) as running_max_monthly_fee,
        count(monthly_fee) over (partition by customer_id order by start_date, id rows between unbounded preceding and current row) as running_count_monthly_fee,
        ntile(5) over (partition by status order by monthly_fee, id) as monthly_fee_bucket
    from filtered_subscriptions
),
base as (
    select *, round(running_count_monthly_fee / running_avg_monthly_fee, 2) as running_count_monthly_fee_per_running_avg_monthly_fee
    from metrics
    where end_date > to_date('2023-07-10')
)
select
    id,
    customer_id,
    end_date,
    monthly_fee,
    status,
    monthly_fee_tier,
    running_avg_monthly_fee,
    running_max_monthly_fee,
    running_count_monthly_fee
from base;
