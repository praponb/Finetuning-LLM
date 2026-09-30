select
    country,
    corr(credit_limit, age) as corr_credit_limit_age,
    sum(credit_limit) filter (where segment = 'Corporate') as corporate_total_credit_limit
from customers
group by country
having corr_credit_limit_age >= 0
order by corr_credit_limit_age desc
