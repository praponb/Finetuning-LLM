with Ranked as (
    select *, row_number() over (partition by customer_id order by started_at desc, id) as rn
    from sessions
)
select
    id,
    customer_id,
    started_at,
    browser
from Ranked
where rn = 1
order by browser;
