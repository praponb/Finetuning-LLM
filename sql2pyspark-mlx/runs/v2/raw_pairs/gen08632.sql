select
    is_remote,
    job_title,
    avg(salary) as avg_salary,
    sort_array(collect_set(job_title)) as job_title_list
from employees
group by 1, 2
