select
    city,
    country,
    round(stddev(rating), 2) as stddev_rating,
    count(*) as row_count
from suppliers
where country in ('JP', 'DE')
group by city, country
order by country;
