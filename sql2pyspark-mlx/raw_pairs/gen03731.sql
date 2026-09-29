with base as (
    -- Keep recent, valid rows
    select
        *,
        case when event_type in ('search', 'page_view') then 'Group A' else 'Other' end as event_type_group,
        lower(event_type) as event_type_lower
    from web_events
    where event_time > date '2023-05-20'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_type_group,
        event_type_lower,
        max(duration_sec) over w_ordered as running_max_duration_sec,
        avg(duration_sec) over w_ordered as running_avg_duration_sec,
        min(duration_sec) over w_ordered as running_min_duration_sec,
        last_value(duration_sec, true) over (partition by session_id order by duration_sec, id rows between unbounded preceding and current row) as last_known_duration_sec
    from base
    window w_ordered as (partition by page, event_type order by duration_sec, id rows between unbounded preceding and current row)
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type_group,
        event_type_lower,
        running_max_duration_sec,
        running_avg_duration_sec,
        running_min_duration_sec,
        last_known_duration_sec,
        cast(last_known_duration_sec as double) as last_known_duration_sec_num
    from metrics
    where running_avg_duration_sec > 130
),
joined as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.event_type_lower,
        s.running_max_duration_sec,
        s.last_known_duration_sec,
        d.country
    from valid s
    inner join sessions d
        on s.session_id = d.id
),
enriched as (
    -- Ranking and gap features
    select
        id,
        running_max_duration_sec,
        country,
        lag(last_known_duration_sec) over w_hist as prev_last_known_duration_sec,
        lag(running_max_duration_sec) over w_hist as prev_running_max_duration_sec,
        lead(running_max_duration_sec) over w_hist as next_running_max_duration_sec,
        row_number() over w_hist as row_seq,
        ntile(4) over (order by last_known_duration_sec desc, id) as last_known_duration_sec_bucket
    from joined
    window w_hist as (partition by session_id, country order by last_known_duration_sec, id)
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        running_max_duration_sec,
        prev_last_known_duration_sec,
        prev_running_max_duration_sec,
        row_seq,
        last_known_duration_sec_bucket,
        prev_running_max_duration_sec * running_max_duration_sec as prev_running_max_duration_sec_times_running_max_duration_sec
    from enriched
    where next_running_max_duration_sec < 270
),
grouped as (
    -- Grouped metrics
    select
        case when prev_running_max_duration_sec_times_running_max_duration_sec >= 77 then 'PREMIUM' else 'STANDARD' end as prev_running_max_duration_sec_times_running_max_duration_sec_tier,
        case when prev_running_max_duration_sec >= 370 then 'LARGE' else 'SMALL' end as prev_running_max_duration_sec_tier,
        count(distinct id) as distinct_ids,
        approx_percentile(last_known_duration_sec_bucket, 0.95) as last_known_duration_sec_bucket_p95,
        approx_percentile(prev_running_max_duration_sec_times_running_max_duration_sec, array(0.25, 0.5, 0.75)) as prev_running_max_duration_sec_times_running_max_duration_sec_quartiles,
        count(*) as num_rows,
        round(avg(last_known_duration_sec_bucket), 2) as avg_last_known_duration_sec_bucket
    from filtered
    group by grouping sets ((case when prev_running_max_duration_sec_times_running_max_duration_sec >= 77 then 'PREMIUM' else 'STANDARD' end, case when prev_running_max_duration_sec >= 370 then 'LARGE' else 'SMALL' end), (case when prev_running_max_duration_sec_times_running_max_duration_sec >= 77 then 'PREMIUM' else 'STANDARD' end), ())
    having round(avg(last_known_duration_sec_bucket), 2) > 0
)
select
    prev_running_max_duration_sec_times_running_max_duration_sec_tier,
    prev_running_max_duration_sec_tier,
    distinct_ids,
    prev_running_max_duration_sec_times_running_max_duration_sec_quartiles,
    avg_last_known_duration_sec_bucket
from grouped
order by prev_running_max_duration_sec_times_running_max_duration_sec_tier;
