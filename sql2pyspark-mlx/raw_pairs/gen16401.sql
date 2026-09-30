with metrics as (
    select
        *,
        row_number() over (partition by event_type order by event_time, id) event_seq,
        last_value(page, true) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) last_known_page
    from web_events
)
select
    id,
    page,
    event_seq,
    last_known_page
from metrics
