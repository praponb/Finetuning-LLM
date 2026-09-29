with enriched as (
    -- Attach sessions attributes
    select
        a.*,
        d.device,
        d.browser
    from web_events a
    left join sessions d
        on a.session_id = d.id
),
enriched_web_events as (
    -- Window metrics over several different partitions
    select
        *,
        avg(duration_sec) over (partition by event_type order by cast(event_time as bigint) range between 604800 preceding and current row) as duration_sec_avg_7d,
        cume_dist() over w_running as duration_sec_cume_dist,
        row_number() over (partition by page order by event_time, id) as row_seq,
        lag(duration_sec, 1, 0) over w_recent as prev_duration_sec
    from enriched
    window w_running as (partition by page order by duration_sec desc nulls last), w_recent as (order by duration_sec, id)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        lag(duration_sec, 2) over (order by event_time, id) as prev_duration_sec_2,
        sum(row_seq) over (partition by browser order by row_seq, id rows between unbounded preceding and current row) as cumulative_row_seq
    from enriched_web_events
)
-- Multi-level aggregation
select
    case when duration_sec_avg_7d >= 466 then 'LARGE' else 'SMALL' end as duration_sec_avg_7d_tier,
    case when prev_duration_sec_2 >= 536 then 'LARGE' else 'SMALL' end as prev_duration_sec_2_tier,
    session_id,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as duration_min,
    avg(duration_sec_avg_7d) filter (where event_type = 'page_view') as page_view_avg_duration_sec_avg_7d,
    sort_array(collect_set(page)) as page_list,
    round(avg(row_seq), 2) as avg_row_seq,
    bit_and(duration_sec) as duration_sec_bit_and,
    max(event_time) as last_event_time
from metrics
group by case when duration_sec_avg_7d >= 466 then 'LARGE' else 'SMALL' end, case when prev_duration_sec_2 >= 536 then 'LARGE' else 'SMALL' end, session_id
order by prev_duration_sec_2_tier desc nulls last
