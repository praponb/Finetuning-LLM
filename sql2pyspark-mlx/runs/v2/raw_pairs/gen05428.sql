select id, device, started_at
from (
    select *, row_number() over (partition by device order by started_at desc, id) pos
    from sessions
    where customer_id = 13
) t
where pos <= 5
order by device desc
