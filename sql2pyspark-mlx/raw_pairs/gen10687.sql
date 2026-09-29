select
    country,
    sum(case when city = 'New York' then rating else 0 end) as new_york_rating,
    count_if(city = 'New York') as new_york_events,
    size(collect_set(country)) as num_countries,
    sort_array(collect_set(city)) as city_list,
    sum(rating) filter (where country = 'UK') as uk_total_rating,
    count(distinct country) as distinct_countries
from suppliers
group by country
having sum(rating) filter (where country = 'UK') >= 0
order by country
