select concat_ws(', ', sort_array(collect_set(brand))) as brand_values from products where size(tags) >= 1
