select job_title, concat_ws(', ', sort_array(collect_set(level))) as level_values
from employees
where salary <= 79000
group by job_title
order by level_values, job_title
