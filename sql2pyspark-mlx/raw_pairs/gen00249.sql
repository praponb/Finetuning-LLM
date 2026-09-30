select
    city,
    country,
    count(case when city = 'London' then 1 end) as london_count,
    round(stddev(rating), 2) as stddev_rating
from suppliers
group by city, country
having stddev_rating > 53
order by london_count asc, stddev_rating asc, city asc, country asc
limit 5;
