select job_title, sort_array(collect_set(job_title)) as job_title_list
from employees
where year(hire_date) >= 2025 and (not is_remote or not is_remote)
group by job_title;
