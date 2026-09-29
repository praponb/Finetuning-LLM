with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        day(started_at) as started_at_day,
        concat(device, ' - ', country) as device_country
    from sessions
    where country = 'UK'
      and started_at > to_date('2023-05-13')
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from valid s
    where exists (
            select 1
            from web_events c
            where c.session_id = s.id
              and c.event_time >= s.started_at
        )
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by country order by started_at, id) > interval '7' day then 1 else 0 end as session_start_flag
    from eligible
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(session_start_flag) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_id
    from gaps
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        max(session_start_flag) over (order by cast(started_at as bigint) range between 604800 preceding and current row) as session_start_flag_max_7d,
        avg(session_id) over (partition by device order by session_start_flag, id rows between 3 preceding and 1 preceding) as session_id_avg_3_prior,
        ntile(4) over (partition by month(started_at), browser order by started_at_day desc, id) as started_at_day_bucket,
        lead(started_at_day, 1) over (order by started_at, id) as next_started_at_day,
        sum(started_at_day) over (order by started_at_day, id rows between unbounded preceding and current row) as cumulative_started_at_day
    from sessionized
    where session_start_flag <= 1
),
eligible_sessions as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time >= s.started_at
        )
),
eligible_sessions_2 as (
    -- Anti-join via a correlated subquery
    select s.*
    from eligible_sessions s
    where not exists (
            select 1
            from web_events ch
            where ch.session_id = s.id
              and ch.event_time <= s.started_at
        )
)
-- Join a dimension and aggregate
select
    c.segment,
    a.browser,
    count(*) as num_rows,
    sum(a.session_id_avg_3_prior) as total_session_id_avg_3_prior,
    grouping_id() as level_id
from eligible_sessions_2 a
left join customers c on a.customer_id = c.id
group by rollup (c.segment, a.browser)
order by browser
