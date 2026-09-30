with flattened as (
    select
        id,
        host,
        log_ts,
        level,
        k,
        value
    from app_logs
    lateral view explode(context) e as k, value
),
with_windows as (
    select
        *,
        last_value(k) ignore nulls over (partition by level order by log_ts, id, k rows between unbounded preceding and current row) as last_known_k,
        first_value(k) ignore nulls over (partition by year(log_ts), k order by log_ts, id, k rows between unbounded preceding and current row) as first_known_k,
        last_value(k) ignore nulls over (order by log_ts, id, k rows between unbounded preceding and current row) as last_known_k_2,
        row_number() over (partition by k order by log_ts, id, k) as seq_no
    from flattened
),
flagged as (
    select *, case when lag(log_ts) over (partition by k order by log_ts, id, k) is null
                 or log_ts > lag(log_ts) over (partition by k order by log_ts, id, k) + interval '30' minute then 1 else 0 end as is_new_session
    from with_windows
),
sessions_app_logs as (
    select *, sum(is_new_session) over (partition by k order by log_ts, id, k rows between unbounded preceding and current row) as island_id
    from flagged
),
cleaned as (
    select
        *,
        length(level) as level_length,
        case when island_id >= 8 then 'High' when island_id >= 3 then 'Medium' else 'Low' end as island_id_band,
        coalesce(k, 'N/A') as resolved_k
    from sessions_app_logs
    where log_ts >= date '2025-06-01'
      and seq_no > 3
)
select
    case when island_id >= 6 then 'HIGH' else 'LOW' end as island_id_tier,
    date_trunc('hour', log_ts) as hour_window,
    case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end as seq_no_tier,
    count(*) filter (where k = 'region') as region_rows,
    max(level_length) as max_level_length,
    sort_array(collect_set(k)) as k_list,
    percentile_approx(level_length, 0.9) as level_length_p90,
    count(distinct host) as distinct_hosts
from cleaned
group by case when island_id >= 6 then 'HIGH' else 'LOW' end, date_trunc('hour', log_ts), case when seq_no >= 8 then 'PREMIUM' else 'STANDARD' end
having count(distinct host) < 1000
