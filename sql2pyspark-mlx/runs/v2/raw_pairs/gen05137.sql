with ranked_sessions as (
    select
        id,
        country,
        started_at,
        rank() over (partition by country order by started_at desc) as rn
    from sessions
)
select id, country, started_at
from ranked_sessions
where rn <= 5
order by country
