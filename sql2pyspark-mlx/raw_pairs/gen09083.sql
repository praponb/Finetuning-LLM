with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        service,
        host,
        level,
        status_code,
        k,
        value
    from app_logs
    lateral view outer explode(context) x as k, value
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, cast(status_code as double) status_code_str
    from exploded
    where k is not distinct from 'version'
      and level = 'ERROR'
),
with_windows as (
    -- Window metrics over several different partitions
    select *, sum(status_code_str) over (partition by k) k_sum_status_code_str
    from filtered
    where level in ('DEBUG', 'INFO', 'WARN')
),
base as (
    -- Basic cleansing and defaults
    select *, status_code / nullif(k_sum_status_code_str, 0) status_code_per_k_sum_status_code_str
    from with_windows
    where level in ('ERROR', 'DEBUG', 'INFO')
),
with_windows_app_logs as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(status_code_str) over (partition by k) k_stddev_samp_status_code_str,
        percent_rank() over (partition by service order by k_sum_status_code_str desc nulls first) k_sum_status_code_str_percent_rank
    from base
),
metrics as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by service, k order by k_stddev_samp_status_code_str desc) k_stddev_samp_status_code_str_dense_rank,
        sum(status_code_per_k_sum_status_code_str) over (partition by level) level_sum_status_code_per_k_sum_status_code_str,
        cume_dist() over (order by k_sum_status_code_str desc nulls first) k_sum_status_code_str_cume_dist
    from with_windows_app_logs
),
matched_keys as (
    -- Set difference on keys
    select id
    from metrics
    except
    select id
    from app_logs
    where service = 'auth'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join matched_keys k on s.id = k.id
)
-- Aggregate with conditional metrics
select
    case when status_code_str >= 77 then 'HIGH' else 'LOW' end as status_code_str_tier,
    case when status_code_per_k_sum_status_code_str >= 85 then 'HIGH' else 'LOW' end as status_code_per_k_sum_status_code_str_tier,
    bit_or(status_code) as status_code_bit_or,
    sum(k_stddev_samp_status_code_str) filter (where host = 'h1') as h1_total_k_stddev_samp_status_code_str,
    count(case when host = 'h3' then 1 end) as h3_count
from reconciled
group by grouping sets ((case when status_code_str >= 77 then 'HIGH' else 'LOW' end, case when status_code_per_k_sum_status_code_str >= 85 then 'HIGH' else 'LOW' end), (case when status_code_str >= 77 then 'HIGH' else 'LOW' end))
having bit_or(status_code) > 1
order by status_code_str_tier desc;
