with base as (
    select
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        round(sqft, 1) as sqft_rounded
    from stores
    where region is not null
      and region is not distinct from 'East'
),
remaining_keys as (
    select city, id
    from base
    intersect all
    select city, id
    from stores
    where city in ('Tokyo', 'New York')
),
reconciled as (
    select s.*
    from base s
    join remaining_keys k on s.city = k.city and s.id = k.id
),
filtered as (
    select
        id,
        store_name,
        city,
        region,
        sqft,
        initcap(trim(store_name)) as store_name_title
    from reconciled
    where region is not distinct from 'West'
),
metrics as (
    select
        *,
        round(sqft / sum(sqft) over (partition by city), 4) as sqft_share,
        round(sqft / sum(sqft) over (partition by region), 4) as sqft_share_2
    from filtered
)
select
    id,
    store_name,
    city,
    region,
    sqft,
    store_name_title,
    sqft_share,
    sqft_share_2
from metrics
order by region nulls last;
