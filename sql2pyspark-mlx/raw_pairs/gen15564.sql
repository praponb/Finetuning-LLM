with cleaned as (
    select
        *,
        lower(event_type) as event_type_lower,
        coalesce(page, 'UNKNOWN') as page_clean
    from web_events
    where event_time > date '2023-08-09'
),
cleaned_web_events as (
    select
        *,
        coalesce(duration_sec, 0) as duration_sec_filled,
        substring(page, 1, 4) as page_prefix
    from cleaned
    where page is not null
),
gaps as (
    select *, case when lag(event_time) over (partition by page order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by page order by event_time, id) + interval '60' day then 1 else 0 end as is_new_session
    from cleaned_web_events
),
sessionized as (
    select
        id,
        page,
        event_time,
        page_clean,
        duration_sec_filled,
        page_prefix,
        sum(is_new_session) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
metrics as (
    select
        *,
        avg(burst_id) over (partition by page order by burst_id, id rows between 6 preceding and 1 preceding) as burst_id_avg_6_prior,
        sum(duration_sec_filled) over (partition by page order by event_time, id rows between unbounded preceding and current row) as cumulative_duration_sec_filled
    from sessionized
)
select
    date_trunc('year', event_time) as year_start,
    date_trunc('quarter', event_time) as quarter_start,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes,
    max(event_time) as last_seen,
    round(sum(duration_sec_filled) / nullif(count(case when page = '/cart' then 1 end), 0), 2) as duration_sec_filled_per_cart,
    sum(case when page = '/search' then burst_id else 0 end) as search_burst_id,
    percent_rank() over (partition by date_trunc('year', event_time) order by (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 desc) as span_minutes_percent_rank
from metrics
group by 1, 2;
