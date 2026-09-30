select
    id,
    start_date,
    plan,
    end_date,
    length(plan) as plan_length
from subscriptions
where start_date >= '2024-10-30'
order by plan, id;
