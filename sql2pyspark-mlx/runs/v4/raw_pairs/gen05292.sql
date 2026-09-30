select id, level, hire_date
from employees
where salary >= 148000
intersect
select id, level, hire_date
from employees
where dept_id = 18
order by hire_date desc;
