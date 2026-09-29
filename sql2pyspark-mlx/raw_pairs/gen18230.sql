select
    city,
    count(distinct city) as distinct_cities,
    sum(case when city = 'Bangkok' then rating else 0 end) as bangkok_rating,
    round(avg(rating), 2) as avg_rating
from suppliers
group by city
order by city desc nulls first
