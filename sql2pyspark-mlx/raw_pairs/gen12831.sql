with remaining_keys as (
    -- Set difference on keys
    select session_id, page
    from web_events
    except
    select session_id, page
    from web_events
    where event_type = 'add_to_cart'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from web_events s
    join remaining_keys k on s.session_id = k.session_id and s.page = k.page
)
select
    s.id,
    s.session_id,
    s.event_type,
    s.duration_sec,
    case when event_type rlike '^(add_to_cart|click)$' then 'GROUP_B' else 'CORE' end as event_type_zone,
    (select avg(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as avg_web_event_duration_sec
from kept_rows s
order by event_type desc nulls last, session_id
