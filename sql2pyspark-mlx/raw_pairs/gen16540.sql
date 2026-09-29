select *
from (
    select *, row_number() over (partition by device order by started_at desc, id) as rn
    from sessions
    where customer_id in (7, 27, 29)
) as t
where rn <= 2
