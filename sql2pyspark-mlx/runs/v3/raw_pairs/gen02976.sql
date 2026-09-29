with numbered as (
    select *, row_number() over (partition by level order by salary, id) as pos
    from employees
    where job_title = 'Intern'
)
select *
from numbered
where pos <= 5;
