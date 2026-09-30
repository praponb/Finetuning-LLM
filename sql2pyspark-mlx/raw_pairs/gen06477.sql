select
    case when salary >= 146000 then 'high' when salary >= 50000 then 'mid' else 'low' end salary_bucket,
    is_remote,
    sort_array(collect_set(job_title)) job_title_list,
    round(stddev(salary), 2) stddev_salary,
    min(hire_date) min_hire_date
from employees
where (year(hire_date) = 2023 and month(hire_date) in (5, 8, 9)) or salary >= 113000
group by 1, 2
having round(stddev(salary), 2) < 35
order by min_hire_date asc
