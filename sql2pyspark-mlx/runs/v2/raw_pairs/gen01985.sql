select job_title, round(stddev(salary), 2) stddev_salary, concat_ws(', ', sort_array(collect_set(level))) level_values from employees group by job_title order by job_title desc
