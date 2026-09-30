select id, dept_id
from employees
where dept_id = 7
union
select id, dept_id
from employees
where salary between 89000 and 130000
order by id desc
