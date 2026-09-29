with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        upper(country) as country_upper,
        coalesce(city, 'DIRECT') as city_clean
    from customers
    where city is not null
      and city is distinct from 'Tokyo'
),
valid_customers as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(name)) as name_title
    from valid
    where city is not null
      and upper(name) like '%OAH%'
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid_customers s
    where not exists (
            select 1
            from accounts ch
            where ch.customer_id = s.id
              and ch.status not in ('CLOSED', 'OPEN')
        )
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        country_upper,
        city_clean,
        name_title,
        age * credit_limit as age_times_credit_limit,
        month(signup_date) as signup_date_month,
        coalesce(city, 'OTHER') as city_clean_2
    from kept
    where signup_date > date '2023-03-04'
),
cleaned_customers as (
    -- Basic cleansing and defaults
    select
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        country_upper,
        name_title,
        age_times_credit_limit,
        signup_date_month,
        round(age_times_credit_limit, 1) as age_times_credit_limit_rounded,
        cast(is_active as int) as is_active_flag
    from cleaned
    where signup_date > to_date('2023-02-05')
)
-- Multi-level aggregation
select
    date_trunc('month', signup_date) as activity_month,
    round(avg(is_active_flag), 2) as avg_is_active_flag,
    count(*) as row_count,
    count(case when city = 'Bangkok' then 1 end) as bangkok_count,
    grouping_id() as grouping_id
from cleaned_customers
group by rollup (date_trunc('month', signup_date))
order by activity_month desc;
