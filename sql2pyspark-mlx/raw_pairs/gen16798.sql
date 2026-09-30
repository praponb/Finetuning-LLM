with matched_keys as (
    -- Set intersection on keys
    select city, id
    from stores
    intersect all
    select city, id
    from stores
    where length(store_name) < 13
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from stores s
    join matched_keys k on s.city = k.city and s.id = k.id
)
select
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    case when store_name rlike '^(Old Town|Mall Central)$' then 'OTHER_ZONE' when store_name rlike '^(Siam Flagship|Downtown Express)$' then 'GROUP_A' else 'EXTENDED' end as store_name_zone
from kept_rows
order by city asc;
