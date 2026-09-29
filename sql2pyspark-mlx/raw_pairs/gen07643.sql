with scored as (
    select
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        level,
        dense_rank() over (partition by dept_id, level order by salary) as salary_dense_rank,
        sum(salary) over (partition by job_title, dept_id order by salary, id rows between unbounded preceding and current row) as cumulative_salary,
        avg(salary) over (partition by level order by salary, id rows between 2 preceding and current row) as salary_avg_2
    from employees
    where dept_id = 16
)
select
    s.id,
    s.full_name,
    s.dept_id,
    s.manager_id,
    s.job_title,
    s.level,
    s.salary_dense_rank,
    s.salary_avg_2,
    case when full_name rlike '^(Henry Ford|bob smith)$' then 'CORE' else 'PRIORITY' end as full_name_zone,
    (select min(t2.salary) from employees t2 where t2.dept_id = s.dept_id) as min_employee_salary
from scored s;
