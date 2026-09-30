with matched_keys as (
    select region
    from stores
    intersect all
    select region
    from stores
    where region in ('Central', 'East', 'West')
),
kept_rows as (
    select s.*
    from stores s
    join matched_keys k on s.region = k.region
),
base as (
    select
        id,
        store_name,
        sqft,
        substring(city, 1, 4) as city_prefix,
        lower(city) as city_lower,
        coalesce(region, 'OTHER') as region_filled
    from kept_rows
    where opened_date >= date '2023-09-06'
      and city = 'Chiang Mai'
),
base_stores as (
    select *, initcap(trim(store_name)) as store_name_title
    from base
    where sqft between 12722 and 14524
),
base_stores_2 as (
    select *, substring(store_name, 1, 2) as store_name_prefix
    from base_stores
    where sqft between 6205 and 9956
),
eligible as (
    select s.*
    from base_stores_2 s
    where not exists (
            select 1
            from store_sales x
            where x.store_id = s.id
              and x.units >= 38
        )
)
select
    id,
    region_filled,
    store_name_title
from eligible
order by id asc nulls last
