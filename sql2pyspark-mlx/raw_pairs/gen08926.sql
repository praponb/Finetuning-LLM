with valid as (
    -- Basic cleansing and defaults
    select
        id,
        store_name,
        city,
        region,
        sqft,
        initcap(trim(store_name)) as store_name_title,
        coalesce(region, 'UNKNOWN') as resolved_region
    from stores
    where opened_date >= date '2023-08-31'
      and region is not null
),
matched_keys as (
    -- Set difference on keys
    select region
    from valid
    except
    select region
    from stores
    where city in ('London', 'Tokyo')
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join matched_keys k on s.region = k.region
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows s
    where not exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.sale_date <= '2024-01-11'
        )
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept s
    where not exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.discount between 0.15 and 0.35
        )
),
kept_keys as (
    -- Set intersection on keys
    select city
    from not_flagged
    intersect all
    select city
    from stores
    where region in ('Central', 'West', 'South')
),
kept_rows_stores as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from not_flagged s
    inner join kept_keys k on s.city = k.city
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from kept_rows_stores s
    where not exists (
            select 1
            from orders x
            where x.store_id = s.id
              and x.store_id = 2
        )
)
-- Grouped metrics
select
    case when sqft >= 17116 then 'LARGE' else 'SMALL' end as sqft_tier,
    sum(case when city = 'New York' then sqft else 0 end) as new_york_sqft,
    size(collect_set(region)) as num_regions
from eligible
group by sqft_tier
order by num_regions desc nulls first;
