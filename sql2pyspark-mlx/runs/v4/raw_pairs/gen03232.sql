with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        attr_key,
        attr_value
    from app_logs
    lateral view outer explode(context) t as attr_key, attr_value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, coalesce(attr_key, 'unassigned') as attr_key_filled
    from lines
    where service in ('search', 'checkout', 'auth')
),
filtered_app_logs as (
    -- Keep recent, valid rows
    select
        *,
        concat_ws('-', service, attr_key) as service_attr_key_key,
        datediff(current_date(), log_ts) as days_since_log_ts
    from filtered
    where attr_key is not null
),
matched_keys as (
    -- Set difference on keys
    select service, id
    from filtered_app_logs
    except
    select service, id
    from app_logs
    where status_code < 325
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from filtered_app_logs s
    join matched_keys k on s.service = k.service and s.id = k.id
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        service,
        log_ts,
        attr_key,
        attr_value,
        attr_key_filled,
        service_attr_key_key,
        days_since_log_ts,
        avg(days_since_log_ts) over w_recent as attr_key_avg_days_since_log_ts,
        sum(days_since_log_ts) over w_hist as attr_key_sum_days_since_log_ts
    from reconciled
    window w_recent as (partition by attr_key), w_hist as (partition by attr_key, service)
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        service,
        log_ts,
        attr_key,
        attr_value,
        attr_key_filled,
        service_attr_key_key,
        days_since_log_ts,
        attr_key_avg_days_since_log_ts,
        attr_key_sum_days_since_log_ts,
        sum(attr_key_avg_days_since_log_ts) over w_part as attr_key_avg_days_since_log_ts_sum_2h,
        count(attr_key_avg_days_since_log_ts) over (partition by attr_key) as attr_key_count_attr_key_avg_days_since_log_ts
    from with_windows
    window w_part as (partition by attr_key, service order by cast(log_ts as bigint) range between 7200 preceding and current row)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(attr_key_sum_days_since_log_ts) over (partition by attr_key, service) as attr_key_sum_attr_key_sum_days_since_log_ts,
        dense_rank() over w_running as attr_key_sum_days_since_log_ts_dense_rank,
        rank() over w_part as attr_key_avg_days_since_log_ts_sum_2h_rank,
        cume_dist() over w_recent as attr_key_sum_days_since_log_ts_cume_dist,
        count(attr_key_avg_days_since_log_ts_sum_2h) over (partition by service, attr_key order by cast(log_ts as bigint) range between 900 preceding and current row) as attr_key_avg_days_since_log_ts_sum_2h_count_15m
    from metrics
    window w_running as (order by attr_key_sum_days_since_log_ts desc nulls last), w_part as (order by attr_key_avg_days_since_log_ts_sum_2h nulls first), w_recent as (partition by service, attr_key order by attr_key_sum_days_since_log_ts desc nulls first)
)
-- Running and moving aggregates
select
    *,
    dense_rank() over w_hist as attr_key_avg_days_since_log_ts_sum_2h_rank_dense_rank,
    cume_dist() over w_grp as days_since_log_ts_cume_dist,
    dense_rank() over (partition by service order by attr_key_sum_attr_key_sum_days_since_log_ts nulls first) as attr_key_sum_attr_key_sum_days_since_log_ts_dense_rank
from enriched
window w_hist as (partition by service order by attr_key_avg_days_since_log_ts_sum_2h_rank desc nulls first), w_grp as (partition by service, attr_key order by days_since_log_ts desc nulls first);
