with top_subscriptions as (
    select *, dense_rank() over (partition by plan order by start_date desc) as pos
    from subscriptions
)
select *
from top_subscriptions
where pos <= 3
order by status
