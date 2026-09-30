select
    id,
    region,
    sqft,
    rank() over (partition by region order by sqft desc) sqft_rank
from stores
where year(opened_date) = 2025
