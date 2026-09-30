select * from (select city, sum(rating) total_rating, concat_ws(', ', sort_array(collect_set(country))) country_values from suppliers group by city) x where total_rating < 23 order by country_values
