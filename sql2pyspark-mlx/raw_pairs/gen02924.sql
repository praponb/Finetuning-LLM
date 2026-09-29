with enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.country,
        c.email,
        c.age,
        c.city
    from sessions e
    inner join customers c
        on e.customer_id <=> c.id
    where c.signup_date >= '2023-12-01'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        email,
        age,
        city,
        regexp_extract(email, '@(.+)$', 1) as email_domain
    from enriched
    where email is null
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        email,
        age,
        city,
        email_domain,
        cast(age as string) as age_num
    from filtered
    where city is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        age,
        city,
        email_domain,
        age_num,
        dense_rank() over (partition by customer_id order by age desc nulls last) as age_dense_rank,
        avg(age) over (order by age, id rows between 5 preceding and current row) as age_avg_5,
        ntile(10) over (partition by city order by age, id) as age_bucket
    from valid
)
-- Grouped metrics
select
    case when age >= 62 then 'LARGE' else 'SMALL' end as age_tier,
    sum(case when city = 'New York' then age_avg_5 else 0 end) as new_york_age_avg_5,
    sort_array(collect_set(city)) as city_list,
    round(avg(age), 2) as avg_age,
    round(avg(age_avg_5), 2) as avg_age_avg_5,
    size(collect_set(city)) as num_cities
from metrics
group by case when age >= 62 then 'LARGE' else 'SMALL' end
order by avg_age_avg_5 desc nulls first;
