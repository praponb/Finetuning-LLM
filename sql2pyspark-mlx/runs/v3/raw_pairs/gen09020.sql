with ranked_subscriptions as (
    select *, row_number() over (partition by plan order by end_date, id) as rnk
    from subscriptions
    where status not in ('CANCELLED', 'ACTIVE')
)
select
    id,
    plan,
    end_date,
    customer_id
from ranked_subscriptions
where rnk <= 5;
