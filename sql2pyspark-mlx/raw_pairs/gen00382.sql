with valid as (
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        cast(started_at as date) as started_at_day,
        coalesce(browser, 'OTHER') as browser_filled
    from sessions
    where customer_id in (12, 21)
),
latest as (
    select *, row_number() over (partition by device, browser order by started_at_day desc nulls last, id) as latest_rank
    from valid
    qualify latest_rank = 1
),
prev_events as (
    select *, lag(started_at) over (partition by browser order by started_at, id) as prev_started_at
    from latest
),
gaps as (
    select *, case when prev_started_at is null or started_at > prev_started_at + interval '7' day then 1 else 0 end as is_new_session
    from prev_events
),
sessionized as (
    select
        id,
        customer_id,
        device,
        browser,
        started_at,
        browser_filled,
        sum(is_new_session) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
with_windows as (
    select
        id,
        browser_filled,
        rank() over (partition by customer_id order by burst_id) as burst_id_rank,
        row_number() over w_recent as event_seq
    from sessionized
    window w_recent as (order by started_at, id)
)
select
    case when event_seq >= 8 then 'LARGE' else 'SMALL' end as event_seq_tier,
    count(distinct id) as distinct_ids,
    count(distinct id) as distinct_ids_2,
    count(distinct id) as distinct_ids_3
from with_windows
group by event_seq_tier
order by distinct_ids_2 desc nulls first;
