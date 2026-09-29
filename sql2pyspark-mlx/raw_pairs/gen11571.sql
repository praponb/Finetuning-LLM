select
    date_format(hire_date, 'yyyy-MM') as hire_month,
    job_title,
    sum(case when level = 'L1' then 1 else 0 end) as l1_count,
    sort_array(collect_set(job_title)) as job_title_list
from employees
where salary > 144000 and is_remote
group by date_format(hire_date, 'yyyy-MM'), job_title
having sum(case when level = 'L5' then 1 else 0 end) >= 56
order by l1_count desc, job_title
