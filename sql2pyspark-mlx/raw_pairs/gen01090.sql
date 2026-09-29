select id, city, sqft
from (
    select *, rank() over (partition by city order by sqft desc) as rnk
    from stores
) as r
where rnk <= 3
order by sqft
