with valid_customers as (
    select *
    from customers
    where not (age >= 41 and country != 'US')
),
Agg as (
    select quarter(signup_date) as signup_quarter, sort_array(collect_set(country)) as country_list
    from valid_customers
    group by quarter(signup_date)
)
select *
from Agg
order by signup_quarter desc
