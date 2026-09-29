select
    id,
    city,
    sqft,
    row_number() over (partition by city order by sqft desc, id) rn
from stores
order by id desc, 4 desc;
