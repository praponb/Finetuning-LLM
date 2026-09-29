with ranked as (
    select
        id,
        device,
        started_at,
        customer_id,
        row_number() over (partition by device order by started_at desc, id) as rnk
    from sessions
    where device not in ('desktop', 'tablet', 'mobile')
)
select
    id,
    device,
    started_at,
    customer_id
from ranked
where rnk <= 2
order by started_at;
