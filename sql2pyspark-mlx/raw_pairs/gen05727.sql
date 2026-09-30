select
    id,
    email,
    segment,
    signup_date,
    credit_limit,
    age,
    avg(age) over w_recent as running_avg_age,
    max(age) over w_recent as running_max_age,
    count(age) over w_recent as running_count_age,
    ntile(5) over w_ordered as credit_limit_bucket
from customers
window w_recent as (partition by city order by signup_date, id rows between unbounded preceding and current row), w_ordered as (partition by segment order by credit_limit, id);
