select
    id,
    browser,
    started_at,
    customer_id
from (
    select *, rank() over (partition by browser order by started_at) as rnk
    from sessions
) as t
where rnk <= 5
