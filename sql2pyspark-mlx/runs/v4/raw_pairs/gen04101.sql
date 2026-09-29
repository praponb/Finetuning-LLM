with matched_keys as (
    -- Set difference on keys
    select city, region
    from stores
    except distinct
    select city, region
    from stores
    where sqft is null
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    inner join matched_keys k on s.city = k.city and s.region = k.region
)
select
    id,
    store_name,
    city,
    region,
    sqft
from verified
