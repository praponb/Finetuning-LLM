with base as (
    select *
    from customers
    where name like '%Chen%' and age between 36 and 40 and email like '%@gmail.com'
),
agg as (
    select year(signup_date) as signup_year, sort_array(collect_set(country)) as country_list, max(signup_date) as last_signup_date
    from base
    group by year(signup_date)
)
select *
from agg
order by signup_year desc
