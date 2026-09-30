with enriched as (
    -- Window metrics over several different partitions
    select
        *,
        row_number() over (partition by year(started_at), browser order by started_at, id) as row_seq,
        last_value(browser) ignore nulls over w_part as last_known_browser,
        last_value(browser) ignore nulls over (partition by device, customer_id order by started_at, id rows between unbounded preceding and current row) as last_known_browser_2
    from sessions
    where browser in ('Safari', 'Chrome')
    window w_part as (order by started_at, id rows between unbounded preceding and current row)
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by country order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by country order by started_at, id) + interval 7 days then 1 else 0 end as is_new_burst
    from enriched
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        last_known_browser,
        last_known_browser_2,
        sum(is_new_burst) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_no
    from gaps
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval 7 days then 1 else 0 end as new_group_flag
    from sessionized
),
sessionized_sessions as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
prev_events as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by country order by started_at, id) as prev_started_at
    from sessionized_sessions
),
gaps_2 as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 7 days then 1 else 0 end as is_new_session
    from prev_events
),
sessions_sessions as (
    -- Running sum of the flag = session number
    select *, sum(is_new_session) over (partition by country order by started_at, id rows between unbounded preceding and current row) as session_id_2
    from gaps_2
),
by_type as (
    -- Native SQL PIVOT
    select *
    from (
        select
            device,
            country,
            new_group_flag
        from sessions_sessions
    ) src
    pivot (
        max(new_group_flag)
        for country in ('TH', 'SG', 'DE')
    )
)
select
    device,
    DE,
    case when device rlike '^(mobile|tablet)$' then 'PRIORITY' else 'GROUP_B' end as device_zone
from by_type
order by DE;
