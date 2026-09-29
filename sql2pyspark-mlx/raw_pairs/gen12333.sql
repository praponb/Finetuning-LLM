with filtered as (
    select *, datediff(current_date(), start_date) / 30 as months_since_start_date
    from subscriptions
    where start_date >= date '2023-09-10'
),
metrics as (
    select
        *,
        sum(monthly_fee) over w_grp as running_sum_monthly_fee,
        avg(monthly_fee) over w_grp as running_avg_monthly_fee,
        count(monthly_fee) over w_grp as running_count_monthly_fee,
        max(monthly_fee) over w_grp as running_max_monthly_fee,
        row_number() over w_recent as event_seq
    from filtered
    window w_grp as (partition by status, customer_id order by monthly_fee, id rows between unbounded preceding and current row), w_recent as (partition by customer_id order by start_date, id)
),
scored as (
    select
        *,
        count(running_sum_monthly_fee) over (partition by customer_id order by start_date, id rows between 2 preceding and current row) as last3_count_running_sum_monthly_fee,
        max(running_sum_monthly_fee) over (partition by customer_id order by start_date, id rows between 2 preceding and current row) as last3_max_running_sum_monthly_fee,
        sum(running_sum_monthly_fee) over (partition by customer_id order by start_date, id rows between 2 preceding and current row) as last3_sum_running_sum_monthly_fee
    from metrics
),
with_windows as (
    select
        id,
        plan,
        start_date,
        monthly_fee,
        months_since_start_date,
        running_count_monthly_fee,
        running_max_monthly_fee,
        event_seq,
        avg(running_avg_monthly_fee) over w_hist as running_avg_running_avg_monthly_fee,
        count(running_avg_monthly_fee) over w_hist as running_count_running_avg_monthly_fee,
        min(running_avg_monthly_fee) over w_hist as running_min_running_avg_monthly_fee
    from scored
    window w_hist as (partition by status, plan order by last3_max_running_sum_monthly_fee, id rows between unbounded preceding and current row)
),
stats as (
    select
        date_trunc('month', start_date) as activity_month,
        date_trunc('week', start_date) as week_start,
        round(sum(months_since_start_date) / nullif(count(case when plan = 'standard' then 1 end), 0), 2) as months_since_start_date_per_standard,
        sort_array(collect_set(plan)) as plan_list,
        max(running_count_running_avg_monthly_fee) as max_running_count_running_avg_monthly_fee
    from with_windows
    group by grouping sets ((date_trunc('month', start_date), date_trunc('week', start_date)), (date_trunc('month', start_date)))
)
select
    activity_month,
    months_since_start_date_per_standard,
    plan_list,
    max_running_count_running_avg_monthly_fee
from stats
