with numbered as (
    select
        id,
        plan,
        start_date,
        row_number() over (partition by plan order by start_date desc, id) as rnk
    from subscriptions
    where end_date < '2024-10-20'
)
select id, plan, start_date
from numbered
where rnk <= 1
order by start_date;
