with with_windows as (
    -- Ranking and gap features
    select
        id,
        browser,
        started_at,
        last_value(country, true) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as last_known_country,
        first_value(country) ignore nulls over w_hist as first_known_country
    from sessions
    window w_hist as (order by started_at, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        datediff(current_date(), started_at) / 365.25 as years_since_started_at,
        coalesce(browser, 'OTHER') as browser_clean
    from with_windows
    where started_at > to_date('2023-07-18')
      and browser is not null
),
latest_match as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.browser,
        s.started_at,
        s.last_known_country,
        s.first_known_country,
        s.years_since_started_at,
        s.browser_clean,
        top1.event_time,
        top1.web_event_duration_sec
    from cleaned s
    left join lateral (
            select c.event_time, c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval '30' day then 1 else 0 end as session_start_flag
    from latest_match
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_no
    from with_gaps
),
enriched as (
    -- Running and moving aggregates
    select
        id,
        browser,
        years_since_started_at,
        browser_clean,
        web_event_duration_sec,
        session_no,
        avg(web_event_duration_sec) over (order by event_time, id rows between 3 preceding and 1 preceding) as web_event_duration_sec_avg_3_prior,
        first_value(years_since_started_at, true) over (partition by browser order by years_since_started_at, id rows between unbounded preceding and current row) as first_known_years_since_started_at
    from sessions_sessions
),
stats as (
    -- Grouped metrics
    select
        session_no,
        max(web_event_duration_sec) filter (where browser = 'Firefox') as firefox_max_web_event_duration_sec,
        approx_percentile(web_event_duration_sec_avg_3_prior, array(0.25, 0.5, 0.75)) as web_event_duration_sec_avg_3_prior_quartiles,
        round(sum(session_no) / nullif(count(case when browser = 'Safari' then 1 end), 0), 2) as session_no_per_safari
    from enriched
    group by session_no
)
select
    session_no,
    firefox_max_web_event_duration_sec,
    web_event_duration_sec_avg_3_prior_quartiles,
    session_no_per_safari
from stats
order by session_no;
