select id, dept_id
from employees
where dept_id in (19, 38) and dept_id in (12, 20)
order by dept_id, id
limit 10;
