with matched_keys as (
    -- Set difference on keys
    select region, city
    from stores
    except all
    select region, city
    from stores
    where city in ('London', 'Berlin')
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    inner join matched_keys k on s.region = k.region and s.city = k.city
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from verified s
    where not exists (
            select 1
            from inventory ch
            where ch.store_id = s.id
              and ch.quantity <> 193
        )
),
kept_keys as (
    -- Set intersection on keys
    select city
    from kept
    intersect
    select city
    from stores
    where sqft between 9664 and 11502
),
verified_stores as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from kept s
    inner join kept_keys k on s.city = k.city
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share,
        dense_rank() over (order by sqft nulls last) as sqft_dense_rank,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share_2
    from verified_stores
    where opened_date <= '2024-09-01'
)
-- Multi-level aggregation
select
    case when sqft_dense_rank >= 8 then 'HIGH' else 'LOW' end as sqft_dense_rank_tier,
    case when sqft_share_2 >= 0.8 then 'HIGH' else 'LOW' end as sqft_share_2_tier,
    date_trunc('week', opened_date) as week_start,
    sum(sqft_dense_rank) filter (where region = 'North') as north_total_sqft_dense_rank,
    round(avg(sqft_share), 2) as avg_sqft_share,
    approx_percentile(sqft_share_2, 0.95) as sqft_share_2_p95,
    bit_or(sqft) as sqft_bit_or,
    count_if(region = 'North') as north_events
from scored
group by grouping sets ((case when sqft_dense_rank >= 8 then 'HIGH' else 'LOW' end, case when sqft_share_2 >= 0.8 then 'HIGH' else 'LOW' end, date_trunc('week', opened_date)), (case when sqft_dense_rank >= 8 then 'HIGH' else 'LOW' end, case when sqft_share_2 >= 0.8 then 'HIGH' else 'LOW' end), ());
