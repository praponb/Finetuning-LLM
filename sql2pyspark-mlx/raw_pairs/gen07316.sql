with filtered as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        lower(country) as country_lower
    from customers
    where signup_date >= date '2023-07-28'
      and segment is not null
),
cleaned as (
    select
        *,
        upper(country) as country_upper,
        coalesce(city, 'UNKNOWN') as city_filled
    from filtered
    where segment is not null
),
metrics as (
    select
        id,
        country,
        segment,
        is_active,
        age,
        country_lower,
        country_upper,
        city_filled,
        round(age / sum(age) over (partition by country), 4) as age_share,
        round(age / sum(age) over (partition by segment), 4) as age_share_2,
        lead(age, 1) over (order by age, id) as next_age,
        sum(age) over (partition by city, segment order by age, id rows between unbounded preceding and current row) as cumulative_age,
        avg(age) over (partition by year(signup_date), city order by signup_date, id rows between 5 preceding and current row) as age_avg_5
    from cleaned
),
metrics_customers as (
    select
        id,
        country_lower,
        country_upper,
        city_filled,
        age_share,
        next_age,
        sum(next_age) over w_hist as running_sum_next_age,
        count(next_age) over w_hist as running_count_next_age,
        avg(next_age) over w_hist as running_avg_next_age
    from metrics
    where age_share is not null
    window w_hist as (partition by country order by age_share, id rows between unbounded preceding and current row)
)
select
    case when next_age >= 68 then 'PREMIUM' else 'STANDARD' end as next_age_tier,
    case when running_avg_next_age >= 58 then 'PREMIUM' else 'STANDARD' end as running_avg_next_age_tier,
    round(avg(running_avg_next_age), 2) as avg_running_avg_next_age,
    round(avg(running_avg_next_age), 2) as avg_running_avg_next_age_2,
    round(avg(running_avg_next_age), 2) as avg_running_avg_next_age_3,
    min(age_share) as min_age_share,
    count(distinct id) as distinct_ids
from metrics_customers
group by next_age_tier, running_avg_next_age_tier
having min(age_share) > 0
order by next_age_tier nulls first;
