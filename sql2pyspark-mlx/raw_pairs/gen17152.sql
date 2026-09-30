with base as (
    select
        id,
        customer_id,
        plan,
        start_date,
        monthly_fee,
        status,
        datediff(end_date, start_date) as days_start_date_to_end_date,
        datediff(current_date(), end_date) / 30 as months_since_end_date
    from subscriptions
    where start_date >= to_date('2023-07-14')
),
latest as (
    select *
    from (
        select *, rank() over (partition by status order by monthly_fee desc) as row_num
        from base
    ) t
    where row_num = 1
),
metrics as (
    select
        *,
        sum(months_since_end_date) over w_recent as running_sum_months_since_end_date,
        max(months_since_end_date) over w_recent as running_max_months_since_end_date,
        avg(months_since_end_date) over w_recent as running_avg_months_since_end_date,
        min(months_since_end_date) over w_recent as running_min_months_since_end_date,
        avg(months_since_end_date) over (partition by year(start_date)) as start_date_year_avg_months_since_end_date
    from latest
    window w_recent as (partition by status, plan order by row_num asc, id asc rows between unbounded preceding and current row)
),
enriched as (
    select
        s.id,
        s.customer_id,
        s.days_start_date_to_end_date,
        s.months_since_end_date,
        s.row_num,
        s.running_sum_months_since_end_date,
        s.running_max_months_since_end_date,
        s.running_avg_months_since_end_date,
        s.start_date_year_avg_months_since_end_date,
        c.age,
        c.city,
        c.country
    from metrics s
    join customers c
        on s.customer_id = c.id
    where c.is_active = true
),
cleaned as (
    select *, floor(age) as age_whole
    from enriched
    where customer_id is not null
      and country is distinct from 'JP'
),
enriched_subscriptions as (
    select e.*, c.email
    from cleaned e
    join customers c
        on e.customer_id = c.id
),
enriched_subscriptions_2 as (
    select
        *,
        lead(running_max_months_since_end_date) over (partition by country order by running_sum_months_since_end_date asc, id asc) as next_running_max_months_since_end_date,
        lag(days_start_date_to_end_date) over (partition by country order by running_sum_months_since_end_date asc, id asc) as prev_days_start_date_to_end_date,
        row_number() over (partition by country order by running_sum_months_since_end_date asc, id asc) as seq_no,
        cume_dist() over (partition by customer_id, country order by days_start_date_to_end_date desc nulls last) as days_start_date_to_end_date_cume_dist
    from enriched_subscriptions
),
totals as (
    select
        case when seq_no >= 9 then 'HIGH' else 'LOW' end as seq_no_tier,
        customer_id,
        approx_percentile(months_since_end_date, array(0.25, 0.5, 0.75)) as months_since_end_date_quartiles,
        round(sum(running_avg_months_since_end_date) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as running_avg_months_since_end_date_per_berlin,
        approx_percentile(age_whole, array(0.25, 0.5, 0.75)) as age_whole_quartiles,
        size(collect_set(country)) as num_countries,
        avg(age) filter (where country = 'US') as us_avg_age,
        round(sum(age_whole) / nullif(count(case when city = 'London' then 1 end), 0), 2) as age_whole_per_london
    from enriched_subscriptions_2
    group by cube (case when seq_no >= 9 then 'HIGH' else 'LOW' end, customer_id)
)
select
    seq_no_tier,
    running_avg_months_since_end_date_per_berlin,
    age_whole_quartiles,
    us_avg_age,
    age_whole_per_london
from totals
order by running_avg_months_since_end_date_per_berlin asc nulls last
