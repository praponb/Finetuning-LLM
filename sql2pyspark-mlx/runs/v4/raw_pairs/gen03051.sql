select
    country,
    city,
    case when city in ('New York', 'Chiang Mai') then 'group_a' else 'other' end as city_group,
    max(rating) as max_rating,
    avg(rating) as avg_rating
from suppliers
where city in ('Singapore', 'London')
group by country, city, case when city in ('New York', 'Chiang Mai') then 'group_a' else 'other' end
having max(rating) < 3.4
