select concat_ws(', ', sort_array(collect_set(category))) as category_values from products where category <> 'Books'
