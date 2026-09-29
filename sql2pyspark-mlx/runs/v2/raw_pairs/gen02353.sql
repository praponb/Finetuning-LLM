select id, city, sqft
from (
    select
        id,
        city,
        sqft,
        dense_rank() over (partition by city order by sqft) as row_num
    from stores
    where year(opened_date) = 2025
) as t
where row_num <= 3
order by city desc
