with lines as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        v
    from app_logs
    lateral view explode(context) li as attr_key, v
),
parsed as (
    -- Struct fields, map lookups and JSON extraction
    select
        id,
        service,
        log_ts,
        level,
        status_code,
        attr_key,
        shiftright(status_code, 2) as status_code_high,
        cast((status_code & 4) != 0 as int) as is_flagged
    from lines
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by service, attr_key order by is_flagged desc nulls first, id, attr_key) as rn
        from parsed
    ) ranked
    where rn = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by level order by rn desc, id, attr_key) as rn_2
        from deduped
    ) x
    where rn_2 = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        status_code,
        attr_key,
        status_code_high,
        rn,
        rn_2,
        cume_dist() over w_hist as is_flagged_cume_dist,
        first_value(attr_key) ignore nulls over (partition by date_trunc('WEEK', log_ts), level order by is_flagged, id, attr_key rows between unbounded preceding and current row) as first_known_attr_key,
        lead(status_code_high, 1, 0) over (partition by service order by rn, id, attr_key) as next_status_code_high
    from ranked
    window w_hist as (partition by attr_key, service order by is_flagged desc)
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        status_code_high + next_status_code_high as status_code_high_plus_next_status_code_high,
        coalesce(attr_key, 'N/A') as attr_key_clean
    from metrics
    where attr_key is distinct from 'version'
)
-- Aggregate with conditional metrics
select
    attr_key,
    min(status_code_high_plus_next_status_code_high) as min_status_code_high_plus_next_status_code_high,
    count_if(attr_key = 'version') as version_events,
    max(is_flagged_cume_dist) filter (where attr_key = 'version') as version_max_is_flagged_cume_dist
from filtered
group by attr_key
order by version_max_is_flagged_cume_dist desc nulls first;
