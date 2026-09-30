select concat_ws(', ', sort_array(collect_set(city))) as city_values from suppliers where (city is not null and supplier_name like '%Par%') or supplier_name is not null
