select id, salary, full_name
from employees
where salary <= (select min(salary) from employees)
order by full_name desc, id
limit 1;
