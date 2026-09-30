with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, length(event_type) as event_type_length
    from web_events
    where event_time >= timestamp '2023-05-06 01:00:00'
      and page is not null
),
joined as (
    -- Attach sessions attributes
    select
        s.id,
        s.session_id,
        s.duration_sec,
        s.event_type_length,
        d.browser,
        d.device
    from cleaned s
    left join sessions d
        on s.session_id = d.id
),
deduped as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by browser, device order by duration_sec desc nulls last, id) as rank_in_group
        from joined
    ) x
    where rank_in_group = 1
),
metrics as (
    -- Ranking and gap features
    select
        *,
        max(event_type_length) over (partition by device order by duration_sec, id rows between 6 preceding and current row) as last7_max_event_type_length,
        count(event_type_length) over (partition by device order by duration_sec, id rows between 6 preceding and current row) as last7_count_event_type_length,
        sum(event_type_length) over (partition by device order by duration_sec, id rows between 6 preceding and current row) as last7_sum_event_type_length,
        min(event_type_length) over (partition by device order by duration_sec, id rows between 6 preceding and current row) as last7_min_event_type_length,
        avg(event_type_length) over (partition by device order by duration_sec, id rows between 5 preceding and current row) as event_type_length_avg_5
    from deduped
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        session_id,
        event_type_length,
        browser,
        device,
        rank_in_group,
        last7_max_event_type_length,
        last7_sum_event_type_length,
        last7_min_event_type_length,
        event_type_length_avg_5,
        sum(last7_count_event_type_length) over (partition by browser order by duration_sec, id rows between unbounded preceding and current row) as running_sum_last7_count_event_type_length,
        max(last7_count_event_type_length) over (partition by browser order by duration_sec, id rows between unbounded preceding and current row) as running_max_last7_count_event_type_length,
        avg(last7_count_event_type_length) over (partition by browser order by duration_sec, id rows between unbounded preceding and current row) as running_avg_last7_count_event_type_length,
        max(last7_sum_event_type_length) over (partition by browser) as browser_max_last7_sum_event_type_length
    from metrics
)
-- Multi-level aggregation
select
    case when event_type_length_avg_5 >= 82 then 'PREMIUM' else 'STANDARD' end as event_type_length_avg_5_tier,
    case when last7_sum_event_type_length >= 330 then 'PREMIUM' else 'STANDARD' end as last7_sum_event_type_length_tier,
    browser,
    size(collect_set(device)) as num_devices,
    approx_percentile(running_sum_last7_count_event_type_length, 0.95) as running_sum_last7_count_event_type_length_p95,
    round(sum(event_type_length) / nullif(count(case when device = 'desktop' then 1 end), 0), 2) as event_type_length_per_desktop,
    sort_array(collect_set(device)) as device_list,
    dense_rank() over (partition by case when event_type_length_avg_5 >= 82 then 'PREMIUM' else 'STANDARD' end order by round(sum(event_type_length) / nullif(count(case when device = 'desktop' then 1 end), 0), 2) desc) as event_type_length_per_desktop_dense_rank
from with_windows
group by 1, 2, 3;
