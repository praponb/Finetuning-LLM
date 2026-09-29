with valid as (
    -- Keep recent, valid rows
    select *, to_date(event_time) as event_time_date
    from web_events
    where event_time >= '2023-08-01'
),
enriched as (
    -- Attach sessions attributes
    select src.*, b.country
    from valid src
    left join sessions b
        on src.session_id = b.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        session_id,
        event_type,
        event_time,
        stddev_samp(duration_sec) over w_ordered as page_stddev_samp_duration_sec,
        cume_dist() over (partition by session_id order by duration_sec desc) as duration_sec_cume_dist
    from enriched
    window w_ordered as (partition by page, country)
),
metrics_web_events as (
    -- Ranking and gap features
    select
        *,
        sum(duration_sec_cume_dist) over (partition by session_id, event_type order by duration_sec_cume_dist asc, id asc rows between unbounded preceding and current row) as running_duration_sec_cume_dist,
        min(page_stddev_samp_duration_sec) over (partition by event_type, session_id) as event_type_min_page_stddev_samp_duration_sec
    from metrics
    where duration_sec_cume_dist >= 0.73
)
-- Multi-level aggregation
select
    case when duration_sec_cume_dist >= 0.77 then 'HIGH' else 'LOW' end as duration_sec_cume_dist_tier,
    case when page_stddev_samp_duration_sec >= 470 then 'PREMIUM' else 'STANDARD' end as page_stddev_samp_duration_sec_tier,
    max(event_time) as last_event_time,
    percentile_approx(event_type_min_page_stddev_samp_duration_sec, array(0.25, 0.5, 0.75)) as event_type_min_page_stddev_samp_duration_sec_quartiles,
    count(case when event_type = 'purchase' then 1 end) as purchase_count
from metrics_web_events
group by case when duration_sec_cume_dist >= 0.77 then 'HIGH' else 'LOW' end, case when page_stddev_samp_duration_sec >= 470 then 'PREMIUM' else 'STANDARD' end
having count(case when event_type = 'purchase' then 1 end) > 1
order by purchase_count asc nulls first
