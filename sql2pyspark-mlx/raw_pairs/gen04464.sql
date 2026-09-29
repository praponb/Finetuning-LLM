with filtered as (
    select
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        initcap(trim(event_type)) as event_type_title,
        coalesce(page, 'OTHER') as resolved_page
    from web_events
    where page is not distinct from '/search'
),
base as (
    select
        *,
        substring(event_type, 1, 3) as event_type_prefix,
        concat_ws(',', event_type, page) as event_type_page_key
    from filtered
    where page is not null
),
enriched as (
    select
        id,
        page,
        event_type_page_key,
        ntile(5) over w_ordered as duration_sec_bucket,
        last_value(duration_sec, true) over w_running as last_known_duration_sec
    from base
    where event_type <> 'search'
    window w_ordered as (partition by session_id order by duration_sec asc, id asc), w_running as (partition by event_type order by duration_sec asc, id asc rows between unbounded preceding and current row)
),
filtered_web_events as (
    select
        *,
        round(last_known_duration_sec / duration_sec_bucket, 2) as last_known_duration_sec_per_duration_sec_bucket,
        coalesce(page, 'UNKNOWN') as resolved_page
    from enriched
    where page is not null
      and last_known_duration_sec <> 202
),
metrics as (
    select
        *,
        max(duration_sec_bucket) over w_ordered as last7_max_duration_sec_bucket,
        avg(duration_sec_bucket) over w_ordered as last7_avg_duration_sec_bucket,
        count(duration_sec_bucket) over w_ordered as last7_count_duration_sec_bucket
    from filtered_web_events
    window w_ordered as (partition by page order by last_known_duration_sec asc, id asc rows between 6 preceding and current row)
),
summary as (
    select
        page,
        approx_percentile(last7_max_duration_sec_bucket, array(0.25, 0.5, 0.75)) as last7_max_duration_sec_bucket_quartiles,
        round(sum(duration_sec_bucket) / nullif(count(case when page = '/home' then 1 end), 0), 2) as duration_sec_bucket_per_home,
        sum(case when page = '/product' then last7_avg_duration_sec_bucket else 0 end) as product_last7_avg_duration_sec_bucket,
        round(avg(last7_count_duration_sec_bucket), 2) as avg_last7_count_duration_sec_bucket,
        count(distinct id) as distinct_ids,
        count(*) as row_count
    from metrics
    group by page
    having count(distinct id) < 1000
)
select page, product_last7_avg_duration_sec_bucket
from summary
order by page asc nulls first, product_last7_avg_duration_sec_bucket desc;
