select id, region, sqft
from (
    select
        id,
        region,
        sqft,
        row_number() over (partition by region order by sqft, id) row_num
    from stores
    where store_name is null
) t
where row_num <= 1;
