select
    id,
    city,
    credit_limit,
    percent_rank() over (partition by city order by credit_limit) as credit_limit_pct_rank
from customers
where credit_limit >= 12300
order by city desc;
