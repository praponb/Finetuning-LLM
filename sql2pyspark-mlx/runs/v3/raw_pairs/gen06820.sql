with ranked as (
    select
        id,
        region,
        sqft,
        store_name,
        row_number() over (partition by region order by sqft desc, id) as row_num
    from stores
)
select
    id,
    region,
    sqft,
    store_name
from ranked
where row_num <= 1
order by 1 desc
