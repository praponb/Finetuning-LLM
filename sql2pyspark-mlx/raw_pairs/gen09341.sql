with base as (
    -- Basic cleansing and defaults
    select
        *,
        length(browser) as browser_length,
        coalesce(country, 'OTHER') as resolved_country
    from sessions
    where started_at > to_date('2023-04-28')
      and started_at >= '2025-01-01'
),
with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, latest.duration_sec
    from base s
    left join lateral (
            select c.duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) latest on true
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by device order by started_at, id) > interval '60' day then 1 else 0 end as is_new_burst
    from with_latest
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        browser_length,
        duration_sec,
        sum(is_new_burst) over (partition by device order by started_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
scored as (
    -- Running and moving aggregates
    select
        *,
        lag(duration_sec) over (partition by device order by duration_sec, id) as prev_duration_sec,
        lag(browser_length) over (partition by device order by duration_sec, id) as prev_browser_length,
        row_number() over (partition by device order by duration_sec, id) as seq_no,
        ntile(3) over (partition by customer_id order by duration_sec desc, id) as duration_sec_bucket
    from sessionized
)
-- Aggregate with conditional metrics
select
    seq_no,
    sum(prev_browser_length) as total_prev_browser_length,
    sum(case when device = 'mobile' then duration_sec else 0 end) as mobile_duration_sec,
    min(started_at) as first_started_at,
    size(collect_set(device)) as num_devices
from scored
group by cube (seq_no)
having mobile_duration_sec > 0;
