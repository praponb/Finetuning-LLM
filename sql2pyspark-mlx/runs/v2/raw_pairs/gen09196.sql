with ranked_subscriptions as (
    select
        id,
        status,
        end_date,
        plan,
        row_number() over (partition by status order by end_date desc, id) as row_num
    from subscriptions
)
select *
from ranked_subscriptions
where row_num <= 5
order by id;
