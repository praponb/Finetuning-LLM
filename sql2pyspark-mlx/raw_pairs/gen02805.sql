select *
from (
    select
        region,
        city,
        sqft
    from stores
) src
pivot (
    sum(sqft) as total,
    count(1) as n
    for city in ('Tokyo', 'Bangkok')
);
