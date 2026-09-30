select concat_ws(', ', sort_array(collect_set(brand))) as brand_values from products order by brand_values desc;
