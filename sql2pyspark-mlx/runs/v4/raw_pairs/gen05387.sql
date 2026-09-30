with lines as (
    select
        id,
        service,
        host,
        log_ts,
        key,
        attr_value
    from app_logs
    lateral view explode(context) t as key, attr_value
),
enriched as (
    select
        *,
        row_number() over (order by log_ts desc, id, key) as row_seq,
        row_number() over (order by log_ts, id, key) as event_seq,
        last_value(key, true) over (partition by host order by log_ts, id, key rows between unbounded preceding and current row) as last_known_key
    from lines
),
cleaned as (
    select
        id,
        service,
        host,
        log_ts,
        key,
        event_seq,
        last_known_key,
        substring(service, 1, 3) as service_prefix
    from enriched
    where log_ts >= timestamp '2025-06-01 07:00:00'
      and key is not null
      and hour(log_ts) between 12 and 16
),
scored as (
    select
        *,
        row_number() over (partition by key order by event_seq, id, key) as seq_no,
        round(event_seq / sum(event_seq) over (partition by key, service), 4) as event_seq_share
    from cleaned
),
with_gaps as (
    select *, case when lag(log_ts) over (partition by key order by log_ts, id, key) is null
                 or log_ts > lag(log_ts) over (partition by key order by log_ts, id, key) + interval '45' minute then 1 else 0 end as session_start_flag
    from scored
),
islands as (
    select *, sum(session_start_flag) over (partition by key order by log_ts, id, key rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
flagged as (
    select *, case when lag(log_ts) over (partition by host order by log_ts, id, key) is null
                 or log_ts > lag(log_ts) over (partition by host order by log_ts, id, key) + interval 20 minutes then 1 else 0 end as session_start_flag_2
    from islands
),
sessions_app_logs as (
    select *, sum(session_start_flag_2) over (partition by host order by log_ts, id, key rows between unbounded preceding and current row) as burst_id_2
    from flagged
),
latest as (
    select *, row_number() over (partition by service, key order by session_start_flag_2 desc nulls first, id, key) as rank_in_group
    from sessions_app_logs
    qualify rank_in_group = 1
)
select
    id,
    service,
    host,
    log_ts,
    key,
    event_seq,
    service_prefix,
    event_seq_share,
    burst_id
from latest
order by service, key
