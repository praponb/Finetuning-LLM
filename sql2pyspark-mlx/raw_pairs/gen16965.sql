select *
from (
    select
        city,
        region,
        sqft
    from stores
) src
pivot (
    avg(sqft)
    for region in ('South', 'North', 'Central')
);
