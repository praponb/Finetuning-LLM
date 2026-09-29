with deduped as (
    -- QUALIFY-style window filter
    select
        id,
        city,
        segment,
        signup_date,
        is_active
    from (
        select *, row_number() over (partition by country order by signup_date, id) as rn
        from customers
    ) t
    where rn = 1
),
remaining_keys as (
    -- Set intersection on keys
    select id, segment
    from deduped
    intersect
    select id, segment
    from customers
    where country = 'UK'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from deduped s
    join remaining_keys k on s.id = k.id and s.segment = k.segment
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, case when city = 'Singapore' then 1 else 0 end as is_singapore
    from verified
    where city is null
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        segment,
        is_active,
        is_singapore,
        min(is_singapore) over (partition by city) as city_min_is_singapore,
        cume_dist() over (partition by city order by is_singapore desc nulls last) as is_singapore_cume_dist
    from base
    where signup_date <= '2023-10-01'
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        segment,
        is_active,
        is_singapore,
        city_min_is_singapore,
        is_singapore_cume_dist,
        floor(is_singapore_cume_dist) as is_singapore_cume_dist_whole,
        coalesce(segment, 'NONE') as segment_filled
    from scored
    where segment is not distinct from 'Consumer'
      and is_singapore >= 52
),
base_customers as (
    -- Filter the raw rows and derive helper columns
    select
        segment,
        is_active,
        is_singapore,
        city_min_is_singapore,
        is_singapore_cume_dist_whole,
        segment_filled,
        is_singapore - is_singapore_cume_dist as is_singapore_minus_is_singapore_cume_dist
    from filtered
    where city_min_is_singapore <= 31
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by segment order by is_singapore_minus_is_singapore_cume_dist desc) as is_singapore_minus_is_singapore_cume_dist_dense_rank,
        rank() over (order by is_singapore desc) as is_singapore_rank,
        cume_dist() over (partition by segment order by is_singapore nulls first) as is_singapore_cume_dist
    from base_customers
),
grouped as (
    -- Aggregate with conditional metrics
    select
        segment,
        count(*) filter (where segment = 'Corporate') as corporate_rows,
        round(sum(is_singapore_minus_is_singapore_cume_dist_dense_rank) / nullif(count(case when segment = 'Corporate' then 1 end), 0), 2) as is_singapore_minus_is_singapore_cume_dist_dense_rank_per_corporate
    from metrics
    group by 1
)
select
    segment,
    corporate_rows,
    is_singapore_minus_is_singapore_cume_dist_dense_rank_per_corporate,
    case when segment rlike '^(SMB|Consumer)$' then 'EXTENDED' when segment rlike '^(Corporate)$' then 'OTHER_ZONE' else 'CORE' end as segment_zone
from grouped;
