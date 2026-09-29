with filtered as (
    select
        *,
        initcap(trim(event_type)) as event_type_title,
        trim(event_type) as clean_event_type
    from web_events
    where event_time > timestamp '2023-08-04 11:00:00'
      and page is not null
),
enriched as (
    select
        *,
        avg(duration_sec) over (order by duration_sec asc, id asc rows between 6 preceding and current row) as duration_sec_avg_6,
        first_value(duration_sec) ignore nulls over (partition by session_id, page order by duration_sec asc, id asc rows between unbounded preceding and current row) as first_known_duration_sec,
        max(duration_sec) over w_grp as duration_sec_max_7d
    from filtered
    window w_grp as (partition by event_type, session_id order by cast(event_time as bigint) asc range between 604800 preceding and current row)
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by page order by event_time asc, id asc) as latest_rank
        from enriched
    ) t
    where latest_rank = 1
),
with_dims as (
    select e.*, b.device
    from ranked e
    left join sessions b
        on e.session_id = b.id
),
enriched_web_events as (
    select
        *,
        row_number() over (partition by session_id order by duration_sec_max_7d asc, id asc) as event_seq,
        row_number() over (partition by event_type, session_id order by event_time asc, id asc) as seq_no,
        ntile(5) over w_ordered as duration_sec_max_7d_bucket
    from with_dims
    window w_ordered as (order by duration_sec_max_7d desc, id asc)
),
with_dims_web_events as (
    select
        src.*,
        s.browser,
        s.country,
        s.device as session_device
    from enriched_web_events src
    left join sessions s
        on src.session_id = s.id
)
select
    id,
    session_id,
    event_time,
    event_type_title,
    duration_sec_avg_6,
    device,
    event_seq,
    seq_no,
    duration_sec_max_7d_bucket
from with_dims_web_events
order by duration_sec_max_7d_bucket desc, event_seq asc
