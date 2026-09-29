select
    signup_month,
    city,
    credit_limit_range,
    last_signup_date
from (
    select
        date_trunc('MONTH', signup_date) as signup_month,
        city,
        max(credit_limit) - min(credit_limit) as credit_limit_range,
        max(signup_date) as last_signup_date
    from customers
    group by date_trunc('MONTH', signup_date), city
) as agg
where credit_limit_range > 48
