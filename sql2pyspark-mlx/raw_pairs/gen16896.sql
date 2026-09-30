select id, device, started_at
from (
    select *, dense_rank() over (partition by device order by started_at desc) as rn
    from sessions
    where customer_id = 11
) as r
where rn <= 3
order by started_at
