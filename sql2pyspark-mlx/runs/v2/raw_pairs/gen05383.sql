select
    id,
    device,
    started_at,
    row_number() over (partition by device order by started_at desc, id) as row_num
from sessions;
