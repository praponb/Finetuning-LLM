select
    quarter(signup_date) as signup_quarter,
    sum(credit_limit) as total_credit_limit,
    count(*) as cnt,
    round(sum(credit_limit) * 100.0 / sum(sum(credit_limit)) over (), 2) as pct_of_total
from customers
where is_active = false
group by quarter(signup_date)
