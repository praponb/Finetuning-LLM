select country, max(rating) - min(rating) as rating_range, concat_ws(', ', sort_array(collect_set(country))) as country_values
from suppliers
where rating >= 3.5 and rating between 1.9 and 2.3
group by country;
