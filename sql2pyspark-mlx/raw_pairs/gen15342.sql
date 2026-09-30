with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        attr_key,
        attr_value
    from carts
    lateral view explode(attributes) x as attr_key, attr_value
),
valid as (
    -- Basic cleansing and defaults
    select
        *,
        length(attr_key) as attr_key_length,
        lower(attr_key) as attr_key_lower,
        coalesce(attr_key, 'UNKNOWN') as resolved_attr_key
    from exploded
    where attr_key is not null
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        attr_key,
        attr_key_length,
        attr_key_lower,
        min(attr_key_length) over (partition by attr_key order by attr_key_length, id, attr_key rows between unbounded preceding and current row) as running_min_attr_key_length,
        sum(attr_key_length) over (partition by attr_key order by attr_key_length, id, attr_key rows between unbounded preceding and current row) as running_sum_attr_key_length,
        max(attr_key_length) over (partition by attr_key order by attr_key_length, id, attr_key rows between unbounded preceding and current row) as running_max_attr_key_length,
        count(attr_key_length) over (partition by attr_key order by attr_key_length, id, attr_key rows between unbounded preceding and current row) as running_count_attr_key_length
    from valid
),
scored as (
    -- Ranking and gap features
    select
        *,
        ntile(3) over (order by running_max_attr_key_length desc, id, attr_key) as running_max_attr_key_length_bucket,
        ntile(4) over (partition by attr_key order by running_min_attr_key_length, id, attr_key) as running_min_attr_key_length_bucket
    from metrics
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        attr_key,
        attr_key_lower,
        running_min_attr_key_length,
        running_max_attr_key_length,
        running_max_attr_key_length_bucket,
        running_min_attr_key_length_bucket,
        row_number() over (partition by attr_key order by running_max_attr_key_length_bucket desc, id, attr_key) as seq_no,
        ntile(3) over (partition by attr_key order by running_min_attr_key_length_bucket desc, id, attr_key) as running_min_attr_key_length_bucket_bucket,
        row_number() over (partition by attr_key order by attr_key_length, id, attr_key) as seq_no_2
    from scored
    where attr_key_length = 75
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        attr_key,
        attr_key_lower,
        running_min_attr_key_length,
        running_max_attr_key_length_bucket,
        running_min_attr_key_length_bucket,
        seq_no,
        running_min_attr_key_length_bucket_bucket,
        seq_no_2,
        floor(running_max_attr_key_length_bucket) as running_max_attr_key_length_bucket_whole,
        coalesce(attr_key, 'N/A') as resolved_attr_key
    from with_windows
    where attr_key is not null
      and attr_key is distinct from 'weight_kg'
),
stats as (
    -- Multi-level aggregation
    select
        attr_key,
        approx_percentile(running_min_attr_key_length_bucket, 0.5) as running_min_attr_key_length_bucket_p50,
        count(*) filter (where attr_key = 'device') as device_rows,
        size(collect_set(attr_key)) as num_attr_keys,
        sum(case when attr_key = 'gift' then seq_no_2 else 0 end) as gift_seq_no_2,
        size(collect_set(attr_key)) as num_attr_keys_2,
        sum(running_max_attr_key_length_bucket_whole) filter (where attr_key = 'weight_kg') as weight_kg_total_running_max_attr_key_length_bucket_whole
    from base
    group by rollup (attr_key)
)
select
    attr_key,
    running_min_attr_key_length_bucket_p50,
    device_rows,
    num_attr_keys,
    gift_seq_no_2,
    num_attr_keys_2
from stats
order by gift_seq_no_2 desc
