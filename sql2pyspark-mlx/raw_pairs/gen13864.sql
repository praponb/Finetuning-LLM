with filtered as (
    select
        id,
        customer_id,
        device,
        started_at,
        country,
        concat_ws('-', country, device) country_device_key
    from sessions
    where started_at > date '2023-08-12'
      and country = 'DE'
),
flagged as (
    select *, case when lag(started_at) over (partition by device order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by device order by started_at asc, id asc) + interval 60 days then 1 else 0 end as new_group_flag
    from filtered
),
sessions_sessions as (
    select
        id,
        device,
        started_at,
        country,
        sum(new_group_flag) over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as session_no
    from flagged
)
select
    case when session_no >= 8 then 'LARGE' else 'SMALL' end as session_no_tier,
    device,
    round(sum(session_no) / nullif(count(case when device = 'desktop' then 1 end), 0), 2) as session_no_per_desktop,
    round(sum(session_no) / nullif(count(case when device = 'tablet' then 1 end), 0), 2) as session_no_per_tablet,
    percentile_approx(session_no, 0.5) as session_no_p50,
    count(*) as row_count,
    count(distinct country) as distinct_countries,
    grouping(case when session_no >= 8 then 'LARGE' else 'SMALL' end) as gid
from sessions_sessions
group by rollup (case when session_no >= 8 then 'LARGE' else 'SMALL' end, device)
order by row_count desc;
