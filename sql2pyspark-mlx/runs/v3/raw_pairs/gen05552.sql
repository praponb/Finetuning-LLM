select year(paid_at) paid_year, method, concat_ws(', ', sort_array(collect_set(method))) method_values from payments group by paid_year, method
