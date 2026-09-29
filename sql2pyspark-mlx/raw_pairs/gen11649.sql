with numbered as (
    select
        id,
        session_id,
        duration_sec,
        event_type,
        row_number() over (partition by session_id order by duration_sec desc, id) rn
    from web_events
)
select *
from numbered
where rn = 1
order by rn desc;
