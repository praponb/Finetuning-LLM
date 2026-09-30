with metrics as (
    -- Window metrics over several different partitions
    select
        id,
        event_time,
        duration_sec,
        avg(duration_sec) over (partition by page order by event_time, id rows between unbounded preceding and current row) as running_avg_duration_sec,
        min(duration_sec) over (partition by page order by event_time, id rows between unbounded preceding and current row) as running_min_duration_sec,
        max(duration_sec) over (partition by page order by event_time, id rows between unbounded preceding and current row) as running_max_duration_sec,
        cume_dist() over (partition by date_trunc('month', event_time), page order by duration_sec nulls first) as duration_sec_cume_dist
    from web_events
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        duration_sec,
        running_avg_duration_sec,
        running_min_duration_sec,
        running_max_duration_sec,
        sum(duration_sec) over (order by event_time, id rows between unbounded preceding and current row) as running_sum_duration_sec,
        count(duration_sec) over (order by event_time, id rows between unbounded preceding and current row) as running_count_duration_sec,
        max(duration_sec) over (order by event_time, id rows between unbounded preceding and current row) as running_max_duration_sec_2,
        ntile(4) over (order by duration_sec_cume_dist desc, id) as duration_sec_cume_dist_bucket
    from metrics
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, round(duration_sec_cume_dist_bucket * 1.15, 2) as duration_sec_cume_dist_bucket_adjusted
    from enriched
    where duration_sec_cume_dist_bucket is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        running_avg_duration_sec,
        running_count_duration_sec,
        duration_sec_cume_dist_bucket_adjusted,
        cume_dist() over w_ordered as running_min_duration_sec_cume_dist,
        row_number() over (order by duration_sec desc, id) as row_seq
    from cleaned
    where running_sum_duration_sec between 1300 and 1400
    window w_ordered as (order by running_min_duration_sec nulls first)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, running_count_duration_sec * running_avg_duration_sec as running_count_duration_sec_times_running_avg_duration_sec
    from scored
    where running_avg_duration_sec >= 330
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        rank() over (order by duration_sec_cume_dist_bucket_adjusted desc nulls first) as duration_sec_cume_dist_bucket_adjusted_rank,
        percent_rank() over (order by running_avg_duration_sec nulls first) as running_avg_duration_sec_percent_rank
    from filtered
),
cleaned_web_events as (
    -- Filter the raw rows and derive helper columns
    select *, running_avg_duration_sec * row_seq as running_avg_duration_sec_times_row_seq
    from with_windows
    where duration_sec_cume_dist_bucket_adjusted < 21
),
stats as (
    -- Aggregate with conditional metrics
    select
        case when row_seq >= 7 then 'LARGE' else 'SMALL' end as row_seq_tier,
        case when running_avg_duration_sec_percent_rank >= 0.84 then 'LARGE' else 'SMALL' end as running_avg_duration_sec_percent_rank_tier,
        sum(running_avg_duration_sec_percent_rank) as total_running_avg_duration_sec_percent_rank,
        count(*) as row_count,
        round(avg(duration_sec_cume_dist_bucket_adjusted_rank), 2) as avg_duration_sec_cume_dist_bucket_adjusted_rank,
        avg(running_min_duration_sec_cume_dist) as avg_running_min_duration_sec_cume_dist,
        grouping_id() as grouping_id
    from cleaned_web_events
    group by rollup (case when row_seq >= 7 then 'LARGE' else 'SMALL' end, case when running_avg_duration_sec_percent_rank >= 0.84 then 'LARGE' else 'SMALL' end)
)
select row_count, avg_duration_sec_cume_dist_bucket_adjusted_rank
from stats;
