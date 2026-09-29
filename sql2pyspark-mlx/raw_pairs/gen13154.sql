with enriched as (
    -- Running and moving aggregates
    select
        *,
        lag(sqft) over (partition by region order by sqft, id) as prev_sqft,
        row_number() over (partition by region order by sqft, id) as seq_no,
        last_value(region, true) over (order by sqft, id rows between unbounded preceding and current row) as last_known_region
    from stores
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from orders c
            where c.store_id = s.id
              and c.amount >= 1210
        )
),
enriched_stores as (
    -- Ranking and gap features
    select
        id,
        store_name,
        city,
        region,
        seq_no,
        last_known_region,
        round(seq_no / sum(seq_no) over (partition by city), 4) as seq_no_share,
        cume_dist() over (order by prev_sqft) as prev_sqft_cume_dist,
        ntile(3) over (order by sqft desc, id) as sqft_bucket,
        row_number() over (partition by region, city order by seq_no, id) as seq_no_2,
        dense_rank() over w_grp as prev_sqft_dense_rank
    from not_flagged
    where city not in ('Tokyo', 'London')
    window w_grp as (partition by city, region order by prev_sqft desc nulls last)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        store_name,
        city,
        region,
        seq_no,
        last_known_region,
        prev_sqft_cume_dist,
        seq_no_2,
        ceil(seq_no_2) as seq_no_2_whole,
        coalesce(region, 'NONE') as region_filled
    from enriched_stores
    where region is not null
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by region order by seq_no_2 desc) as latest_rank
        from base
    ) t
    where latest_rank = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        city,
        seq_no,
        seq_no_2,
        row_number() over (order by seq_no desc, id) as seq_no_3,
        sum(seq_no_2) over (order by latest_rank, id rows between unbounded preceding and current row) as cumulative_seq_no_2,
        lag(seq_no_2, 1) over (partition by region order by latest_rank, id) as prev_seq_no_2
    from latest
),
eligible as (
    -- Semi-join via a correlated subquery
    select s.*
    from with_windows s
    where exists (
            select 1
            from inventory x
            where x.store_id = s.id
              and x.last_restock > '2024-01-01'
        )
)
select
    id,
    city,
    seq_no,
    seq_no_2,
    seq_no_3,
    cumulative_seq_no_2,
    prev_seq_no_2
from eligible
order by city desc
