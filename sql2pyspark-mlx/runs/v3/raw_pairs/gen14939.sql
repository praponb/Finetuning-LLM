select
    date_trunc('MONTH', signup_date) as signup_month,
    round((100 * sum(case when city = 'Bangkok' then 1 else 0 end)) / count(*), 1) as pct_bangkok,
    round(avg(credit_limit), 2) as avg_credit_limit,
    sum(case when country = 'SG' then credit_limit else 0 end) as sg_credit_limit
from customers
group by signup_month
having sg_credit_limit > 49
order by avg_credit_limit desc
