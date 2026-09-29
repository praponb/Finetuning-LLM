select *
from (
    select
        region,
        city,
        sqft
    from stores
) src
pivot (
    max(sqft) as total
    for city in ('Singapore' as singapore, 'Chiang Mai' as chiang_mai)
);
