select id, device, started_at
from (
    select *, dense_rank() over (partition by device order by started_at desc) rn
    from sessions
) t
where rn = 1
order by 3 desc;
