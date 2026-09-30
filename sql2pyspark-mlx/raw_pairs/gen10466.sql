with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        page,
        initcap(trim(page)) as page_title,
        round(duration_sec * 100, 2) as duration_sec_scaled
    from web_events
    where page is not null
),
metrics as (
    -- Ranking and gap features
    select
        id,
        page_title,
        count(duration_sec_scaled) over w_hist as last7_count_duration_sec_scaled,
        min(duration_sec_scaled) over w_hist as last7_min_duration_sec_scaled,
        avg(duration_sec_scaled) over w_hist as last7_avg_duration_sec_scaled,
        cume_dist() over (partition by page order by duration_sec_scaled nulls last) as duration_sec_scaled_cume_dist
    from cleaned
    where page != '/cart'
    window w_hist as (partition by page order by duration_sec_scaled, id rows between 6 preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        page_title,
        max(duration_sec_scaled_cume_dist) over w_ordered as running_max_duration_sec_scaled_cume_dist,
        sum(duration_sec_scaled_cume_dist) over w_ordered as running_sum_duration_sec_scaled_cume_dist,
        min(duration_sec_scaled_cume_dist) over w_ordered as running_min_duration_sec_scaled_cume_dist,
        avg(duration_sec_scaled_cume_dist) over w_ordered as running_avg_duration_sec_scaled_cume_dist
    from metrics
    window w_ordered as (order by duration_sec_scaled_cume_dist, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        *,
        running_min_duration_sec_scaled_cume_dist - running_max_duration_sec_scaled_cume_dist as running_min_duration_sec_scaled_cume_dist_minus_running_max_duration_sec_scaled_cume_dist,
        round(running_max_duration_sec_scaled_cume_dist * 0.9, 2) as running_max_duration_sec_scaled_cume_dist_adjusted
    from enriched
    where running_sum_duration_sec_scaled_cume_dist >= 2.5
)
-- Aggregate with conditional metrics
select
    case when running_max_duration_sec_scaled_cume_dist_adjusted >= 78 then 'LARGE' else 'SMALL' end as running_max_duration_sec_scaled_cume_dist_adjusted_tier,
    sum(running_max_duration_sec_scaled_cume_dist_adjusted) as total_running_max_duration_sec_scaled_cume_dist_adjusted,
    count(*) as cnt
from base
group by running_max_duration_sec_scaled_cume_dist_adjusted_tier
