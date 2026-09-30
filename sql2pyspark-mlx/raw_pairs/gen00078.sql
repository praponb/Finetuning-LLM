with ranked_stores as (
    select *, row_number() over (partition by city order by sqft desc, id) as row_num
    from stores
)
select
    id,
    city,
    sqft,
    opened_date
from ranked_stores
where row_num <= 2
order by city, sqft
