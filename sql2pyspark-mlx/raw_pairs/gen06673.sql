with base as (
    -- Keep recent, valid rows
    select
        id,
        session_id,
        page,
        event_time,
        date_format(event_time, 'yyyy-MM') as event_time_label,
        coalesce(page, 'OTHER') as page_filled
    from web_events
    where page is not null
      and page is distinct from '/search'
),
base_web_events as (
    -- Keep recent, valid rows
    select *, case when page in ('/checkout', '/product') then 'Priority' else 'Other' end as page_group
    from base
    where page is not null
      and session_id in (1, 17)
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        page,
        event_time_label,
        page_filled,
        page_group,
        row_number() over w_ordered as row_seq,
        row_number() over (partition by session_id, page order by event_time desc, id) as row_seq_2
    from base_web_events
    window w_ordered as (partition by page order by event_time, id)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by session_id order by row_seq_2 desc, id) as dedup_rank
        from with_windows
    ) t
    where dedup_rank = 1
),
matched_keys as (
    -- Set difference on keys
    select session_id
    from latest
    except all
    select session_id
    from web_events
    where page in ('/search', '/checkout', '/cart')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    join matched_keys k on s.session_id = k.session_id
)
select
    page,
    page_filled,
    page_group,
    row_seq_2,
    dedup_rank
from verified
order by row_seq_2 desc
