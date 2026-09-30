select
    job_title,
    case when job_title in ('Engineer', 'Analyst') then 'core' else 'other' end job_title_group,
    min(salary) min_salary,
    count(case when level = 'L5' then 1 end) l5_count,
    sort_array(collect_set(job_title)) job_title_list
from employees
where salary >= 57000 or full_name is not null
group by 1, 2
having sum(case when job_title = 'Engineer' then salary else 0 end) < 22
order by l5_count desc
