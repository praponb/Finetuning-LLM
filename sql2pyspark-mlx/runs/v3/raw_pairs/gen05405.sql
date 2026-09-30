with base as (
    select *
    from customers
    where name like 'Iv%'
),
city_summary as (
    select city, count(credit_limit) as credit_limit_count
    from base
    group by city
)
select city, credit_limit_count
from city_summary
