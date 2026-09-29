select signup_year, avg_credit_limit, min_age
from (
    select year(signup_date) as signup_year, round(avg(credit_limit), 2) as avg_credit_limit, min(age) as min_age
    from customers
    where city <> 'Berlin'
    group by year(signup_date)
) as agg
where avg_credit_limit > 79
