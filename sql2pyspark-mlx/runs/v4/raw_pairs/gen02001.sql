with base as (
    -- Basic cleansing and defaults
    select *, case when status in ('CANCELLED', 'PAUSED') then 'Core' else 'Other' end as status_group
    from subscriptions
    where customer_id is not null
),
base_subscriptions as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), start_date) as days_since_start_date,
        concat(status, ', ', plan) as status_plan
    from base
    where customer_id is not null
),
base_subscriptions_2 as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        status_group,
        days_since_start_date,
        status_plan,
        date_format(end_date, 'yyyy-MM') as end_date_label
    from base_subscriptions
    where end_date >= to_date('2023-08-02')
      and days_since_start_date < 20
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        lag(monthly_fee, 1) over (partition by status order by monthly_fee, id) as prev_monthly_fee,
        lead(days_since_start_date, 1) over (partition by customer_id, plan order by end_date, id) as next_days_since_start_date
    from base_subscriptions_2
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(days_since_start_date) over (partition by status) as status_stddev_samp_days_since_start_date,
        sum(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between unbounded preceding and current row) as running_monthly_fee,
        avg(monthly_fee) over (order by start_date, id rows between 5 preceding and 1 preceding) as monthly_fee_avg_5_prior,
        round(prev_monthly_fee / sum(prev_monthly_fee) over (partition by customer_id), 4) as prev_monthly_fee_share
    from with_windows
),
with_windows_subscriptions as (
    -- Running and moving aggregates
    select
        *,
        avg(next_days_since_start_date) over w_running as status_avg_next_days_since_start_date,
        avg(prev_monthly_fee_share) over w_hist as plan_avg_prev_monthly_fee_share,
        avg(status_stddev_samp_days_since_start_date) over (partition by plan order by prev_monthly_fee, id rows between 5 preceding and current row) as status_stddev_samp_days_since_start_date_avg_5,
        avg(prev_monthly_fee) over w_running as status_avg_prev_monthly_fee,
        avg(monthly_fee_avg_5_prior) over (partition by plan order by monthly_fee, id rows between 5 preceding and 1 preceding) as monthly_fee_avg_5_prior_avg_5_prior
    from metrics
    window w_running as (partition by status), w_hist as (partition by plan, customer_id)
)
select
    id,
    customer_id,
    start_date,
    status,
    days_since_start_date,
    status_plan,
    end_date_label,
    next_days_since_start_date,
    status_stddev_samp_days_since_start_date
from with_windows_subscriptions
order by id desc, status_stddev_samp_days_since_start_date desc;
