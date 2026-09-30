with ranked_sessions as (
    select
        id,
        device,
        started_at,
        country,
        rank() over (partition by device order by started_at desc) pos
    from sessions
)
select
    id,
    device,
    started_at,
    country
from ranked_sessions
where pos <= 1;
