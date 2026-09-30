with valid_customers as (
    select *, upper(trim(email)) as email_normalized
    from customers
    where name like '%FRANK%'
),
customers_stats as (
    select segment, max(signup_date) as last_signup_date
    from valid_customers
    group by segment
)
select *
from customers_stats
order by last_signup_date;
