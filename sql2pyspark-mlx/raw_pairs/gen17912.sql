with valid as (
    -- Basic cleansing and defaults
    select
        *,
        to_date(started_at) as started_at_date,
        initcap(trim(browser)) as browser_title
    from sessions
    where browser is not null
      and country is not distinct from 'UK'
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country
    from (
        select *, row_number() over (partition by country, browser order by started_at_date asc, id asc) as rank_in_group
        from valid
    ) t
    where rank_in_group = 1
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.duration_sec,
        x.page
    from deduped s
    left join lateral (
            select c.duration_sec, c.page
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) x on true
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_last s
    where exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time < s.started_at
        )
)
-- Grouped metrics
select
    case when duration_sec >= 476 then 'LARGE' else 'SMALL' end as duration_sec_tier,
    date_trunc('month', started_at) as activity_month,
    (unix_timestamp(max(started_at)) - unix_timestamp(min(started_at))) / 60 as duration_min,
    count(*) as num_rows,
    sum(case when page = '/product' then duration_sec else 0 end) as product_duration_sec
from not_flagged
group by rollup (case when duration_sec >= 476 then 'LARGE' else 'SMALL' end, date_trunc('month', started_at))
having duration_min > 0
order by duration_min desc nulls last;
