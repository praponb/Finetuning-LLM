select location, count(budget) budget_count, concat_ws(', ', sort_array(collect_set(dept_name))) dept_name_values, count(*) total_count from departments group by location order by budget_count desc;
