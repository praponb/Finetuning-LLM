select
    city,
    sum(case when country = 'DE' then 1 else 0 end) as de_count,
    sum(case when city = 'New York' then rating else 0 end) as new_york_rating,
    round(avg(rating), 2) as avg_rating
from suppliers
where rating is not null
group by city;
