select
    country,
    max(rating) - min(rating) as rating_range,
    count(distinct rating) as unique_citys,
    count(*) as total_count
from suppliers
group by country
having min(rating) >= 1.9;
