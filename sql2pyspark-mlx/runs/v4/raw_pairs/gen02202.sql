with numbered as (
    select *, row_number() over (partition by status order by start_date desc, id) as rn
    from subscriptions
)
select
    id,
    status,
    start_date,
    plan
from numbered
where rn <= 2;
