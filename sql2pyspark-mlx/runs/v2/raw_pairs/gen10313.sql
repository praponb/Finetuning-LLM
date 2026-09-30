select
    id,
    country,
    signup_date,
    credit_limit,
    round(avg(credit_limit) over (partition by country order by signup_date, id rows between 3 preceding and current row), 2) as credit_limit_moving_avg_4
from customers
where country in ('UK', 'SG', 'US')
order by country, signup_date desc;
