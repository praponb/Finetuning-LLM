select
    quarter(hire_date) as hire_quarter,
    job_title,
    count(distinct salary) as unique_depts,
    sort_array(collect_set(level)) as level_list
from employees
where level = 'L3' and (level = 'L4' or email is not null)
group by quarter(hire_date), job_title
order by job_title;
