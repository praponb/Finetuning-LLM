select
    is_remote,
    case when salary >= 158000 then 'high' when salary >= 69000 then 'mid' else 'low' end as salary_bucket,
    avg(salary) as avg_salary,
    sort_array(collect_set(job_title)) as job_title_list
from employees
where not (not is_remote and salary < 128000)
group by is_remote, case when salary >= 158000 then 'high' when salary >= 69000 then 'mid' else 'low' end
