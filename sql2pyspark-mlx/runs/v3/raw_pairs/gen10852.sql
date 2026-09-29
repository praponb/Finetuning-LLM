with base as (
    select *
    from customers
    where segment = 'Corporate'
),
agg as (
    select city, min(signup_date) as first_signup_date
    from base
    group by city
)
select *
from agg
order by first_signup_date;
