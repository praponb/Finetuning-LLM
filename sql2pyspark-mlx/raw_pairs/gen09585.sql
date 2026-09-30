select city, max(rating) max_rating, concat_ws(', ', sort_array(collect_set(country))) country_values from suppliers group by city order by city desc;
