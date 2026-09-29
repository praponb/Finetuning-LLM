select
    country,
    date_trunc('MONTH', signup_date) signup_month,
    city,
    min(signup_date) first_signup_date
from customers
where signup_date >= '2025-08-01' and credit_limit is not null and name like '%smit%'
group by 1, 2, 3
order by signup_month;
