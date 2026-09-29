select
    id,
    name,
    city,
    is_active,
    age,
    round(credit_limit / sum(credit_limit) over (partition by country), 4) as credit_limit_share,
    rank() over (partition by city order by credit_limit desc) as credit_limit_rank,
    avg(credit_limit) over (order by age, id rows between 3 preceding and current row) as credit_limit_avg_3
from customers
