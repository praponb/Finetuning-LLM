with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        trim(page) as clean_page,
        floor(duration_sec) as duration_sec_whole
    from web_events
    where page is not null
),
latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by session_id order by duration_sec_whole) as rank_in_group
    from filtered
    qualify rank_in_group = 1
),
metrics as (
    -- Ranking and gap features
    select
        id,
        event_type,
        page,
        clean_page,
        first_value(duration_sec_whole, true) over (partition by session_id order by duration_sec_whole, id rows between unbounded preceding and current row) as first_known_duration_sec_whole,
        ntile(4) over (partition by page order by duration_sec_whole, id) as duration_sec_whole_bucket
    from latest
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by event_type order by first_known_duration_sec_whole nulls first) as first_known_duration_sec_whole_dense_rank,
        count(duration_sec_whole_bucket) over (partition by page) as page_count_duration_sec_whole_bucket,
        ntile(3) over (order by duration_sec_whole_bucket, id) as duration_sec_whole_bucket_bucket
    from metrics
)
-- Native SQL PIVOT
select *
from (
    select
        page,
        event_type,
        first_known_duration_sec_whole
    from enriched
) src
pivot (
    sum(first_known_duration_sec_whole) as amount,
    count(1) as item_count
    for event_type in ('purchase' as purchase, 'click' as click, 'add_to_cart' as add_to_cart)
)
