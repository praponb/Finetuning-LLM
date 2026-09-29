with filtered as (
    select
        *,
        trim(device) clean_device,
        coalesce(country, 'DIRECT') country_clean
    from sessions
    where started_at > date '2023-05-18'
)
select
    date_trunc('year', started_at) as year_start,
    count_if(device = 'mobile') as mobile_events,
    count(case when device = 'desktop' then 1 end) as desktop_count,
    max(started_at) as last_seen,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as span_minutes
from filtered
group by date_trunc('year', started_at)
order by year_start desc nulls last;
