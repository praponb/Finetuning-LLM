select location, min(budget) as min_budget, concat_ws(', ', sort_array(collect_set(dept_name))) as dept_name_values from departments group by location
