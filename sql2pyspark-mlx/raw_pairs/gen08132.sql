select
    job_title,
    level,
    is_remote,
    count(distinct salary) unique_job_titles,
    count(case when job_title = 'Engineer' then 1 end) engineer_count,
    sum(case when level = 'L4' then 1 else 0 end) l4_count
from employees
group by job_title, level, is_remote;
