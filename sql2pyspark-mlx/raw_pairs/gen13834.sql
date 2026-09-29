with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        initcap(trim(status)) status_title,
        add_months(end_date, 12) end_date_plus_months
    from subscriptions
    where customer_id is not null
      and customer_id in (33, 36)
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_title,
        end_date_plus_months,
        case when monthly_fee >= 206 then 'High' when monthly_fee >= 47 then 'Medium' else 'Low' end monthly_fee_band
    from cleaned
    where customer_id = 15
),
cleaned_subscriptions as (
    -- Basic cleansing and defaults
    select
        *,
        cast(monthly_fee as int) monthly_fee_int,
        datediff(end_date, start_date) / 7 weeks_start_date_to_end_date
    from valid
    where customer_id is not null
      and start_date >= '2024-03-01'
),
scored as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        monthly_fee,
        status_title,
        end_date_plus_months,
        monthly_fee_band,
        avg(monthly_fee_int) over (partition by status order by start_date, id rows between unbounded preceding and current row) running_avg_monthly_fee_int,
        sum(monthly_fee_int) over (partition by status order by start_date, id rows between unbounded preceding and current row) running_sum_monthly_fee_int,
        count(monthly_fee_int) over (partition by status order by start_date, id rows between unbounded preceding and current row) running_count_monthly_fee_int,
        ntile(3) over (partition by customer_id order by monthly_fee_int, id) monthly_fee_int_bucket
    from cleaned_subscriptions
)
-- Aggregate with conditional metrics
select
    date_trunc('week', end_date_plus_months) as week_start,
    case when running_sum_monthly_fee_int >= 340 then 'LARGE' else 'SMALL' end as running_sum_monthly_fee_int_tier,
    customer_id,
    approx_percentile(running_avg_monthly_fee_int, 0.5) as running_avg_monthly_fee_int_p50,
    round(avg(running_count_monthly_fee_int), 2) as avg_running_count_monthly_fee_int,
    avg(running_avg_monthly_fee_int) as avg_running_avg_monthly_fee_int,
    rank() over (partition by date_trunc('week', end_date_plus_months) order by avg(running_avg_monthly_fee_int) desc) as avg_running_avg_monthly_fee_int_rank
from scored
group by date_trunc('week', end_date_plus_months), case when running_sum_monthly_fee_int >= 340 then 'LARGE' else 'SMALL' end, customer_id
