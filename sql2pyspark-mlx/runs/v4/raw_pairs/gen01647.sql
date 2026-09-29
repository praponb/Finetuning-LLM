with with_latest as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.page,
        top1.web_event_duration_sec
    from sessions s
    left join lateral (
            select c.page, c.duration_sec as web_event_duration_sec
            from web_events c
            where c.session_id = s.id
              and c.event_time <= s.started_at
            order by c.event_time desc, c.id desc
            limit 1
        ) top1 on true
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        sum(web_event_duration_sec) over (partition by customer_id order by cast(started_at as bigint) range between 604800 preceding and current row) as web_event_duration_sec_sum_7d,
        lead(web_event_duration_sec, 1) over w_part as next_web_event_duration_sec,
        sum(web_event_duration_sec) over w_running as cumulative_web_event_duration_sec,
        ntile(5) over (order by web_event_duration_sec desc, id) as web_event_duration_sec_bucket,
        row_number() over w_part_2 as row_seq
    from with_latest
    window w_part as (partition by customer_id order by started_at, id), w_running as (partition by device order by web_event_duration_sec, id rows between unbounded preceding and current row), w_part_2 as (partition by device order by web_event_duration_sec, id)
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        b.credit_limit,
        b.city,
        b.name
    from enriched s
    left join customers b
        on s.customer_id = b.id
),
matched as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from with_dims src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.started_at >= sub.start_date
       and src.started_at < sub.end_date + interval '14' day
),
enriched_sessions as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by page order by monthly_fee nulls last) as monthly_fee_cume_dist,
        rank() over (partition by page order by cumulative_web_event_duration_sec nulls first) as cumulative_web_event_duration_sec_rank,
        sum(web_event_duration_sec) over (order by next_web_event_duration_sec, id, subscription_id rows between unbounded preceding and current row) as running_web_event_duration_sec,
        rank() over (partition by device order by cumulative_web_event_duration_sec desc) as cumulative_web_event_duration_sec_rank_2
    from matched
),
prev_events as (
    -- Previous event time per key
    select *, lag(started_at) over (partition by plan order by started_at, id, subscription_id) as prev_started_at
    from enriched_sessions
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_started_at is null or started_at > prev_started_at + interval 90 days then 1 else 0 end as new_group_flag
    from prev_events
),
sessionized as (
    -- Running sum of the flag = session number
    select *, sum(new_group_flag) over (partition by plan order by started_at, id, subscription_id rows between unbounded preceding and current row) as session_id
    from with_gaps
)
-- Multi-level aggregation
select
    page,
    case when running_web_event_duration_sec >= 1806 then 'HIGH' else 'LOW' end as running_web_event_duration_sec_tier,
    count(*) as event_count,
    (unix_timestamp(max(prev_started_at)) - unix_timestamp(min(prev_started_at))) / 60 as span_minutes,
    count(*) filter (where browser = 'Chrome') as chrome_rows,
    round(avg(row_seq), 2) as avg_row_seq,
    (unix_timestamp(max(prev_started_at)) - unix_timestamp(min(prev_started_at))) / 60 as duration_min,
    min(prev_started_at) as first_seen,
    grouping(page) as grouping_level
from sessionized
group by rollup (page, case when running_web_event_duration_sec >= 1806 then 'HIGH' else 'LOW' end)
order by event_count nulls first
