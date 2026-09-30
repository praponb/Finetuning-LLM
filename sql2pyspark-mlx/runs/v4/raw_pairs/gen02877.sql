with kept_keys as (
    select id
    from web_events
    except distinct
    select id
    from web_events
    where event_type != 'page_view'
),
kept_rows as (
    select s.*
    from web_events s
    join kept_keys k on s.id = k.id
)
select
    id,
    event_type,
    page,
    duration_sec
from kept_rows;
