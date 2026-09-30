with valid as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        event_type,
        case when page in ('/home', '/search') then 'Priority' else 'Other' end as page_group,
        coalesce(page, 'UNKNOWN') as resolved_page
    from web_events
    where page is distinct from '/product'
      and event_time >= timestamp '2023-08-25 18:00:00'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        event_type,
        page_group,
        resolved_page,
        length(event_type) as event_type_length
    from valid
    where session_id = 29
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lead(event_type_length) over w_part as next_event_type_length,
        row_number() over w_part as seq_no
    from filtered
    window w_part as (partition by event_type order by event_type_length, id)
),
ranked as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by event_type order by next_event_type_length, id) as rn
    from metrics
    qualify rn = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        event_type,
        resolved_page,
        event_type_length,
        next_event_type_length,
        seq_no,
        count(next_event_type_length) over w_hist as running_count_next_event_type_length,
        min(next_event_type_length) over w_hist as running_min_next_event_type_length,
        sum(next_event_type_length) over w_hist as running_sum_next_event_type_length
    from ranked
    window w_hist as (partition by event_type order by next_event_type_length, id rows between unbounded preceding and current row)
)
select
    id,
    event_type,
    resolved_page,
    running_count_next_event_type_length,
    running_min_next_event_type_length
from scored;
