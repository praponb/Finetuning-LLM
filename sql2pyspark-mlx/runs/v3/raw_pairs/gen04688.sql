with base as (
    select *
    from customers
    where credit_limit < 14400 and city = 'Singapore' and city <> 'Bangkok'
),
signup_month_summary as (
    select date_trunc('MONTH', signup_date) as signup_month, case when age >= 63 then 'high' when age >= 33 then 'mid' else 'low' end as age_bucket, avg(age) as avg_age
    from base
    group by date_trunc('MONTH', signup_date), case when age >= 63 then 'high' when age >= 33 then 'mid' else 'low' end
)
select signup_month, age_bucket, avg_age
from signup_month_summary
