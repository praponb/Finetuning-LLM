with valid as (
    -- Keep recent, valid rows
    select
        *,
        case when dept_name = 'Finance' then 1 else 0 end as is_finance,
        coalesce(dept_name, 'OTHER') as dept_name_filled
    from departments
    where location is not null
      and budget >= 1984000
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        ntile(3) over w_recent as is_finance_bucket,
        max(is_finance) over (partition by location) as location_max_is_finance,
        ntile(5) over (partition by location order by is_finance asc, id asc) as is_finance_bucket_2,
        rank() over (partition by dept_name order by is_finance asc nulls first) as is_finance_rank
    from valid
    where dept_name in ('Marketing', 'Engineering')
    window w_recent as (order by is_finance desc, id asc)
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(is_finance_bucket_2, true) over (partition by dept_name, location order by is_finance asc, id asc rows between unbounded preceding and current row) as last_known_is_finance_bucket_2,
        ntile(10) over (order by is_finance asc, id asc) as is_finance_bucket_3
    from with_windows
),
latest as (
    -- Keep one row per key (latest first)
    select
        id,
        is_finance_bucket,
        is_finance_bucket_2,
        is_finance_rank,
        last_known_is_finance_bucket_2
    from (
        select *, row_number() over (partition by location, dept_name order by location_max_is_finance desc nulls last, id asc) as dedup_rank
        from enriched
    ) t
    where dedup_rank = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        is_finance_bucket,
        is_finance_bucket_2,
        is_finance_rank,
        min(last_known_is_finance_bucket_2) over w_ordered as last3_min_last_known_is_finance_bucket_2,
        count(last_known_is_finance_bucket_2) over w_ordered as last3_count_last_known_is_finance_bucket_2,
        max(last_known_is_finance_bucket_2) over w_ordered as last3_max_last_known_is_finance_bucket_2,
        avg(last_known_is_finance_bucket_2) over w_ordered as last3_avg_last_known_is_finance_bucket_2
    from latest
    window w_ordered as (order by is_finance_bucket_2 asc, id asc rows between 2 preceding and current row)
),
kept_keys as (
    -- Set intersection on keys
    select id
    from metrics
    intersect
    select id
    from departments
    where budget between 3406000 and 3759000
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join kept_keys k on s.id = k.id
),
scored as (
    -- Ranking and gap features
    select
        id,
        is_finance_bucket_2,
        is_finance_rank,
        last3_min_last_known_is_finance_bucket_2,
        last3_count_last_known_is_finance_bucket_2,
        last3_max_last_known_is_finance_bucket_2,
        percent_rank() over w_running as is_finance_bucket_2_percent_rank,
        dense_rank() over (order by last3_min_last_known_is_finance_bucket_2 desc nulls last) as last3_min_last_known_is_finance_bucket_2_dense_rank,
        cume_dist() over w_part as last3_min_last_known_is_finance_bucket_2_cume_dist,
        percent_rank() over w_part_2 as last3_count_last_known_is_finance_bucket_2_percent_rank
    from kept_rows
    window w_running as (order by is_finance_bucket_2 desc nulls first), w_part as (order by last3_min_last_known_is_finance_bucket_2 desc), w_part_2 as (order by last3_count_last_known_is_finance_bucket_2 desc nulls last)
),
stats as (
    -- Multi-level aggregation
    select
        case when last3_min_last_known_is_finance_bucket_2_cume_dist >= 0.68 then 'LARGE' else 'SMALL' end as last3_min_last_known_is_finance_bucket_2_cume_dist_tier,
        round(avg(is_finance_rank), 2) as avg_is_finance_rank,
        count(*) as event_count,
        count(distinct id) as distinct_ids,
        percentile_approx(last3_max_last_known_is_finance_bucket_2, 0.95) as last3_max_last_known_is_finance_bucket_2_p95
    from scored
    group by 1
)
select
    last3_min_last_known_is_finance_bucket_2_cume_dist_tier,
    avg_is_finance_rank,
    event_count,
    distinct_ids,
    last3_max_last_known_is_finance_bucket_2_p95
from stats
order by distinct_ids desc nulls first, last3_max_last_known_is_finance_bucket_2_p95 asc;
