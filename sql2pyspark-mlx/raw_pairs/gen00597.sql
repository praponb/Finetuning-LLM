select
    job_title,
    max(salary) max_salary,
    min(salary) min_salary,
    concat_ws(', ', sort_array(collect_set(level))) level_values
from employees
group by job_title
order by job_title
limit 5;
