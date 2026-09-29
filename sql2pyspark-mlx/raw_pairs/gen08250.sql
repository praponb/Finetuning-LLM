with with_windows as (
    select
        id,
        customer_id,
        plan,
        monthly_fee,
        status,
        count(monthly_fee) over (partition by status order by end_date asc, id asc rows between unbounded preceding and current row) as running_count_monthly_fee,
        min(monthly_fee) over (partition by status order by end_date asc, id asc rows between unbounded preceding and current row) as running_min_monthly_fee,
        max(monthly_fee) over (partition by status order by end_date asc, id asc rows between unbounded preceding and current row) as running_max_monthly_fee,
        sum(monthly_fee) over (partition by status order by end_date asc, id asc rows between unbounded preceding and current row) as running_sum_monthly_fee
    from subscriptions
),
valid as (
    select
        *,
        round(running_count_monthly_fee, 1) as running_count_monthly_fee_rounded,
        coalesce(running_sum_monthly_fee, 0) as running_sum_monthly_fee_filled
    from with_windows
    where customer_id is not null
      and customer_id = 32
),
deduped as (
    select *, row_number() over (partition by plan, status order by running_count_monthly_fee asc nulls last, id asc) as dedup_rank
    from valid
    qualify dedup_rank = 1
),
with_windows_subscriptions as (
    select
        *,
        sum(running_min_monthly_fee) over (partition by plan order by running_sum_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_sum_running_min_monthly_fee,
        min(running_min_monthly_fee) over (partition by plan order by running_sum_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_min_running_min_monthly_fee,
        count(running_min_monthly_fee) over (partition by plan order by running_sum_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_count_running_min_monthly_fee
    from deduped
    where running_count_monthly_fee_rounded between 60 and 75
),
scored as (
    select
        *,
        max(running_min_monthly_fee) over (partition by status, customer_id order by running_min_running_min_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_max_running_min_monthly_fee,
        avg(running_min_monthly_fee) over (partition by status, customer_id order by running_min_running_min_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_avg_running_min_monthly_fee,
        min(running_min_monthly_fee) over (partition by status, customer_id order by running_min_running_min_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_min_running_min_monthly_fee_2,
        count(running_min_monthly_fee) over (partition by status, customer_id order by running_min_running_min_monthly_fee asc, id asc rows between unbounded preceding and current row) as running_count_running_min_monthly_fee_2
    from with_windows_subscriptions
),
cleaned as (
    select
        *,
        ceil(running_min_monthly_fee) as running_min_monthly_fee_whole,
        dedup_rank * running_count_running_min_monthly_fee_2 as dedup_rank_times_running_count_running_min_monthly_fee_2
    from scored
    where customer_id is not null
)
select
    p.city,
    s.status,
    count(*) as num_rows,
    max(s.running_count_monthly_fee_rounded) as max_running_count_monthly_fee_rounded,
    grouping_id() as gid
from cleaned s
left join customers p on s.customer_id = p.id
group by grouping sets ((p.city, s.status), (p.city));
