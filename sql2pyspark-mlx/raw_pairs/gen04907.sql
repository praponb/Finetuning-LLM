with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        concat_ws('/', event_type, page) as event_type_page_key
    from web_events
    where event_time >= timestamp '2023-05-05 18:00:00'
      and page is not null
),
enriched as (
    -- Attach sessions attributes
    select
        e.id,
        e.session_id,
        e.page,
        e.event_time,
        e.duration_sec,
        s.country,
        s.device
    from base e
    left join sessions s
        on e.session_id = s.id
),
joined as (
    -- Attach sessions attributes
    select s.*, d.country as session_country
    from enriched s
    inner join sessions d
        on s.session_id = d.id
    where to_date(d.started_at) >= '2025-08-01'
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by device order by duration_sec nulls last, id) as rank_in_group
    from joined
    qualify rank_in_group = 1
),
scored as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_time,
        country,
        rank_in_group,
        stddev_samp(duration_sec) over (partition by session_id, device) as session_id_stddev_samp_duration_sec,
        sum(duration_sec) over (partition by session_id order by rank_in_group, id rows between unbounded preceding and current row) as cumulative_duration_sec,
        stddev_samp(duration_sec) over w_ordered as session_id_stddev_samp_duration_sec_2
    from deduped
    window w_ordered as (partition by session_id)
),
with_dims as (
    -- Attach sessions attributes
    select
        e.id,
        e.session_id,
        e.country,
        e.session_id_stddev_samp_duration_sec,
        e.cumulative_duration_sec,
        d.country as session_country,
        d.device
    from scored e
    left join sessions d
        on e.session_id = d.id
)
-- Ranking and gap features
select
    *,
    ntile(3) over (partition by session_id order by session_id_stddev_samp_duration_sec, id) as session_id_stddev_samp_duration_sec_bucket,
    row_number() over (partition by session_country, device order by session_id_stddev_samp_duration_sec desc, id) as event_seq
from with_dims;
