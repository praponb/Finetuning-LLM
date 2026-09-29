select id, country, started_at
from (
    select
        id,
        country,
        started_at,
        row_number() over (partition by country order by started_at desc, id) as row_num
    from sessions
) as ranked
where row_num = 1
