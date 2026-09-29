with kept_keys as (
    select country
    from customers
    except all
    select country
    from customers
    where age > 34
),
verified as (
    select s.*
    from customers s
    inner join kept_keys k on s.country = k.country
),
base as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        case when is_active then 'Yes' else 'No' end as is_active_label,
        last_day(signup_date) as signup_date_month_end,
        coalesce(city, 'UNKNOWN') as city_filled
    from verified
    where city is not null
),
cleaned as (
    select *, datediff(signup_date, signup_date_month_end) / 7 as weeks_signup_date_month_end_to_signup_date
    from base
    where city is not null
      and city is distinct from 'London'
),
scored as (
    select
        *,
        dense_rank() over (partition by month(signup_date_month_end), city order by weeks_signup_date_month_end_to_signup_date desc nulls last) as weeks_signup_date_month_end_to_signup_date_dense_rank,
        round(credit_limit / sum(credit_limit) over (partition by country, segment), 4) as credit_limit_share
    from cleaned
)
select
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    age,
    is_active_label
from scored
order by city desc nulls last
