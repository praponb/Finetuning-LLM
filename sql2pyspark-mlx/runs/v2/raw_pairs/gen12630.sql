select
    id,
    city,
    sqft,
    region,
    dense_rank() over (partition by city order by sqft desc) as dense_rnk,
    row_number() over (partition by region order by sqft desc, id) as rn
from stores
where city in ('Bangkok', 'Singapore', 'Chiang Mai')
order by dense_rnk desc, id desc;
