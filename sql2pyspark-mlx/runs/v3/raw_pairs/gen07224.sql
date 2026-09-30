select *
from (
    select
        id,
        region,
        sqft,
        store_name,
        row_number() over (partition by region order by sqft, id) pos
    from stores
) ranked
where pos = 1
order by sqft desc, region;
