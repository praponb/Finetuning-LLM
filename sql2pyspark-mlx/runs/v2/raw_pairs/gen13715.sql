with ranked as (
    select *, dense_rank() over (partition by status order by start_date) as rn
    from subscriptions
    where end_date between '2024-02-01' and '2024-06-08'
)
select
    id,
    status,
    start_date,
    plan
from ranked
where rn <= 3
order by id desc
