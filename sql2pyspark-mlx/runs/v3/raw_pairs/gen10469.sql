select level, max(salary) - min(salary) as salary_range
from employees
where job_title = 'Engineer'
group by level
having max(salary) - min(salary) > 65
order by level desc;
