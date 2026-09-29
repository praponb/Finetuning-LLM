with base as (
    -- Basic cleansing and defaults
    select
        *,
        upper(store_name) as store_name_upper,
        coalesce(region, 'DIRECT') as region_clean
    from stores
    where opened_date >= to_date('2023-06-02')
      and sqft between 6661 and 7039
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.channel = 'web'
        )
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        sqft,
        store_name_upper,
        region_clean,
        initcap(trim(city)) as city_title
    from kept
    where opened_date > date '2023-03-23'
      and region is not null
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from valid s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.amount <= 710
        )
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        first_value(region) ignore nulls over w_part as first_known_region,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share,
        lead(sqft, 1) over w_recent as next_sqft,
        avg(sqft) over (partition by region, city order by sqft asc, id asc rows between 5 preceding and 1 preceding) as sqft_avg_5_prior,
        sum(sqft) over (partition by region order by sqft asc, id asc rows between unbounded preceding and current row) as cumulative_sqft
    from not_flagged
    window w_part as (partition by city order by sqft asc, id asc rows between unbounded preceding and current row), w_recent as (order by sqft asc, id asc)
),
summary as (
    -- Multi-level aggregation
    select
        case when next_sqft >= 14856 then 'PREMIUM' else 'STANDARD' end as next_sqft_tier,
        city,
        count_if(city = 'Tokyo') as tokyo_events,
        round(sum(sqft_share) / nullif(count(case when city = 'Berlin' then 1 end), 0), 2) as sqft_share_per_berlin,
        count(*) as num_rows
    from enriched
    group by grouping sets ((case when next_sqft >= 14856 then 'PREMIUM' else 'STANDARD' end, city), (case when next_sqft >= 14856 then 'PREMIUM' else 'STANDARD' end), ())
)
select
    next_sqft_tier,
    city,
    tokyo_events,
    sqft_share_per_berlin,
    num_rows
from summary;
