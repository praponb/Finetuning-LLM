with valid as (
    select
        id,
        event_time,
        duration_sec,
        concat_ws('/', event_type, page) as event_type_page_key,
        date_trunc('YEAR', event_time) as event_time_period,
        coalesce(page, 'UNKNOWN') as page_clean
    from web_events
    where page is not null
      and page is not distinct from '/home'
),
metrics as (
    select
        id,
        duration_sec,
        event_type_page_key,
        event_time_period,
        avg(duration_sec) over (order by duration_sec asc, id asc rows between 3 preceding and current row) as last4_avg_duration_sec,
        max(duration_sec) over (order by duration_sec asc, id asc rows between 3 preceding and current row) as last4_max_duration_sec,
        sum(duration_sec) over (order by duration_sec asc, id asc rows between 3 preceding and current row) as last4_sum_duration_sec,
        first_value(duration_sec, true) over (partition by year(event_time) order by event_time asc, id asc rows between unbounded preceding and current row) as first_known_duration_sec
    from valid
),
filtered as (
    select
        *,
        cast(first_known_duration_sec as double) as first_known_duration_sec_str,
        ceil(duration_sec) as duration_sec_whole
    from metrics
    where last4_max_duration_sec >= 460
),
metrics_web_events as (
    select
        *,
        lag(last4_max_duration_sec) over w_grp as prev_last4_max_duration_sec,
        lag(duration_sec_whole) over w_grp as prev_duration_sec_whole,
        row_number() over w_grp as event_seq,
        avg(first_known_duration_sec) over (order by first_known_duration_sec_str asc, id asc rows between 5 preceding and 1 preceding) as first_known_duration_sec_avg_5_prior
    from filtered
    window w_grp as (order by last4_avg_duration_sec asc, id asc)
),
metrics_web_events_2 as (
    select
        *,
        last_value(first_known_duration_sec, true) over (order by last4_sum_duration_sec asc, id asc rows between unbounded preceding and current row) as last_known_first_known_duration_sec,
        ntile(5) over (order by first_known_duration_sec asc, id asc) as first_known_duration_sec_bucket,
        sum(duration_sec_whole) over (partition by date_trunc('month', event_time_period) order by prev_duration_sec_whole asc, id asc rows between unbounded preceding and current row) as cumulative_duration_sec_whole
    from metrics_web_events
),
valid_web_events as (
    select
        id,
        duration_sec,
        event_time_period,
        last4_avg_duration_sec,
        last4_max_duration_sec,
        last4_sum_duration_sec,
        first_known_duration_sec_str,
        duration_sec_whole,
        prev_duration_sec_whole,
        first_known_duration_sec_avg_5_prior,
        last_known_first_known_duration_sec,
        first_known_duration_sec_bucket,
        cumulative_duration_sec_whole,
        round(duration_sec * 0.85, 2) as duration_sec_adjusted
    from metrics_web_events_2
    where last4_avg_duration_sec < 480
)
select
    case when last4_max_duration_sec >= 440 then 'LARGE' else 'SMALL' end as last4_max_duration_sec_tier,
    bit_or(duration_sec) as duration_sec_bit_or,
    percentile_approx(last_known_first_known_duration_sec, array(0.25, 0.5, 0.75)) as last_known_first_known_duration_sec_quartiles
from valid_web_events
group by cube (case when last4_max_duration_sec >= 440 then 'LARGE' else 'SMALL' end)
