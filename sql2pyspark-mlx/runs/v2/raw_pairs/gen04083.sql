with ranked as (
    select
        id,
        city,
        sqft,
        store_name,
        row_number() over (partition by city order by sqft desc, id) row_num
    from stores
)
select
    id,
    city,
    sqft,
    store_name
from ranked
where row_num <= 3
