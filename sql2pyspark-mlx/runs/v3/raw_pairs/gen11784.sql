select
    id,
    country,
    signup_date,
    segment,
    age,
    first_value(segment) over (partition by country order by signup_date, id) as first_segment,
    round(age / sum(age) over (partition by segment), 3) as age_share
from customers
where country not in ('SG', 'US', 'JP')
order by id desc
