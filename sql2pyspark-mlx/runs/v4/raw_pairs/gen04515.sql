with flattened as (
    -- Unnest the array into one row per element
    select
        id,
        log_ts,
        status_code,
        k,
        attr_value
    from app_logs
    lateral view outer explode(context) li as k, attr_value
),
parsed as (
    -- Semi-structured parsing
    select
        k,
        attr_value,
        cast((status_code & 1) != 0 as int) as is_flagged,
        shiftright(status_code, 2) as status_code_high
    from flattened
),
with_windows as (
    -- Window metrics over several different partitions
    select
        k,
        attr_value,
        is_flagged,
        status_code_high,
        stddev_samp(is_flagged) over (partition by k) as k_stddev_samp_is_flagged,
        sum(status_code_high) over (partition by k) as k_sum_status_code_high,
        percent_rank() over (partition by k order by status_code_high desc nulls first) as status_code_high_percent_rank,
        rank() over (order by status_code_high desc nulls last) as status_code_high_rank
    from parsed
),
totals as (
    -- Multi-level aggregation
    select
        k,
        count(*) as cnt,
        sort_array(collect_set(k)) as k_list,
        size(collect_set(k)) as num_ks,
        max(is_flagged) filter (where k = 'version') as version_max_is_flagged
    from with_windows
    group by k
    having version_max_is_flagged >= 0
)
select
    k,
    cnt,
    k_list,
    num_ks,
    case when k rlike '^(version|region)$' then 'OTHER_ZONE' when k rlike '^(trace)$' then 'EXTENDED' else 'CORE' end as k_zone
from totals
order by cnt desc nulls last;
