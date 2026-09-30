select
    is_remote,
    sum(case when job_title = 'Director' then salary else 0 end) as director_salary,
    sum(salary) as total_salary,
    count(distinct salary) as unique_managers
from employees
where level != 'L2' and is_remote
group by is_remote
order by total_salary desc, director_salary desc
